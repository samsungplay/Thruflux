package app.thruflux.android

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.Intent
import android.content.pm.ServiceInfo
import android.os.Build
import android.os.IBinder
import android.util.Log
import java.io.File
import java.net.HttpURLConnection
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.net.URL
import java.util.concurrent.atomic.AtomicBoolean

class EngineService : Service() {
    private var engineProcess: Process? = null
    private var enginePort: Int? = null
    private var heartbeatServer: ServerSocket? = null
    private var workerThread: Thread? = null
    private val stopping = AtomicBoolean(false)
    private var currentStatus = "Starting"

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        when (intent?.action) {
            ACTION_STOP -> {
                Log.i(SERVICE_TAG, "Stop requested")
                stopEngine()
                stopSelf()
                return START_NOT_STICKY
            }
            ACTION_UPDATE_TRANSFER -> {
                if (workerThread?.isAlive != true) {
                    stopSelf()
                    return START_NOT_STICKY
                }
                updateNotification(
                    title = intent.getStringExtra(EXTRA_NOTIFICATION_TITLE) ?: "Thruflux transfer",
                    text = intent.getStringExtra(EXTRA_NOTIFICATION_TEXT) ?: "Transfer active",
                    bigText = intent.getStringExtra(EXTRA_NOTIFICATION_BIG_TEXT),
                    ongoing = true,
                )
            }
            ACTION_CLEAR_TRANSFER -> {
                if (workerThread?.isAlive == true) {
                    updateNotification(currentStatus)
                } else {
                    stopSelf()
                    return START_NOT_STICKY
                }
            }
            else -> startEngineIfNeeded()
        }
        return START_STICKY
    }

    override fun onDestroy() {
        stopEngine()
        super.onDestroy()
    }

    private fun startEngineIfNeeded() {
        if (workerThread?.isAlive == true) {
            Log.i(SERVICE_TAG, "Start ignored because worker is already active")
            return
        }
        stopping.set(false)
        EngineStatus.setState(EngineState.Starting)
        currentStatus = "Starting"
        startForegroundCompat(notification("Starting"))
        workerThread = Thread {
            try {
                val heartbeatPort = startHeartbeatServer()
                val apiPort = reserveFreePort()
                enginePort = apiPort
                EngineStatus.setBaseUrl("http://127.0.0.1:$apiPort")
                val binary = File(applicationInfo.nativeLibraryDir, ENGINE_BINARY_NAME)
                Log.i(SERVICE_TAG, "Starting engine binary=${binary.absolutePath} apiPort=$apiPort heartbeatPort=$heartbeatPort")
                if (!binary.exists()) {
                    throw IllegalStateException("Engine binary not found: ${binary.absolutePath}")
                }
                binary.setExecutable(true)
                engineProcess = ProcessBuilder(
                    binary.absolutePath,
                    "ui",
                    "--port",
                    apiPort.toString(),
                    "--ui-heartbeat-port",
                    heartbeatPort.toString(),
                )
                    .redirectErrorStream(true)
                    .start()
                val process = engineProcess
                Thread {
                    process?.inputStream?.bufferedReader()?.use { reader ->
                        while (!stopping.get()) {
                            val line = reader.readLine() ?: break
                            Log.i(ENGINE_TAG, line)
                        }
                    }
                }.start()
                if (waitForHealth(apiPort)) {
                    Log.i(SERVICE_TAG, "Engine health check succeeded")
                    EngineStatus.setState(EngineState.Ready)
                    currentStatus = "Ready"
                    updateNotification("Ready")
                    val exitCode = engineProcess?.waitFor()
                    if (!stopping.get()) {
                        Log.e(SERVICE_TAG, "Engine exited unexpectedly exitCode=$exitCode")
                        EngineStatus.setState(EngineState.Failed)
                        EngineStatus.setBaseUrl(null)
                        currentStatus = "Not ready"
                        updateNotification("Not ready")
                    }
                } else {
                    throw IllegalStateException("Engine health check timed out")
                }
            } catch (throwable: Throwable) {
                if (!stopping.get()) {
                    Log.e(SERVICE_TAG, "Engine startup failed", throwable)
                    EngineStatus.setState(EngineState.Failed)
                    EngineStatus.setBaseUrl(null)
                    currentStatus = "Not ready"
                    updateNotification("Not ready")
                }
            }
        }
        workerThread?.start()
    }

    private fun stopEngine() {
        stopping.set(true)
        Log.i(SERVICE_TAG, "Stopping engine")
        runCatching { engineProcess?.destroy() }
        engineProcess = null
        runCatching { heartbeatServer?.close() }
        heartbeatServer = null
        workerThread = null
        enginePort = null
        EngineStatus.setBaseUrl(null)
        EngineStatus.setState(EngineState.Stopped)
        stopForeground(STOP_FOREGROUND_REMOVE)
    }

    private fun startHeartbeatServer(): Int {
        val server = ServerSocket(0, 16, InetAddress.getByName("127.0.0.1"))
        heartbeatServer = server
        Log.i(SERVICE_TAG, "Heartbeat server listening on ${server.localPort}")
        Thread {
            while (!stopping.get()) {
                val socket = runCatching { server.accept() }.getOrNull() ?: break
                handleHeartbeat(socket)
            }
        }.start()
        return server.localPort
    }

    private fun handleHeartbeat(socket: Socket) {
        socket.use {
            val body = "ok"
            val response = "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: ${body.length}\r\nConnection: close\r\n\r\n$body"
            it.getOutputStream().write(response.toByteArray(Charsets.UTF_8))
        }
    }

    private fun reserveFreePort(): Int {
        ServerSocket(0, 16, InetAddress.getByName("127.0.0.1")).use {
            return it.localPort
        }
    }

    private fun waitForHealth(port: Int): Boolean {
        val deadline = System.currentTimeMillis() + ENGINE_START_TIMEOUT_MS
        while (System.currentTimeMillis() < deadline && !stopping.get()) {
            if (isHealthy(port)) {
                return true
            }
            Thread.sleep(ENGINE_POLL_INTERVAL_MS)
        }
        return false
    }

    private fun isHealthy(port: Int): Boolean {
        return runCatching {
            val connection = URL("http://127.0.0.1:$port/health").openConnection() as HttpURLConnection
            connection.connectTimeout = ENGINE_HEALTH_TIMEOUT_MS
            connection.readTimeout = ENGINE_HEALTH_TIMEOUT_MS
            connection.requestMethod = "GET"
            val ok = connection.responseCode in 200..299
            connection.disconnect()
            ok
        }.onFailure {
            Log.d(SERVICE_TAG, "Health check failed on port $port: ${it.message}")
        }.getOrDefault(false)
    }

    private fun startForegroundCompat(notification: Notification) {
        createNotificationChannel()
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
            startForeground(NOTIFICATION_ID, notification, ServiceInfo.FOREGROUND_SERVICE_TYPE_DATA_SYNC)
        } else {
            startForeground(NOTIFICATION_ID, notification)
        }
    }

    private fun updateNotification(status: String) {
        currentStatus = status
        updateNotification("Thruflux engine", status, null, status != "Not ready")
    }

    private fun updateNotification(title: String, text: String, bigText: String?, ongoing: Boolean) {
        val manager = getSystemService(NotificationManager::class.java)
        manager.notify(NOTIFICATION_ID, notification(title, text, bigText, ongoing))
    }

    private fun notification(status: String): Notification {
        return notification("Thruflux engine", status, null, status != "Not ready")
    }

    private fun notification(title: String, text: String, bigText: String?, ongoing: Boolean): Notification {
        createNotificationChannel()
        val stopIntent = Intent(this, EngineService::class.java).setAction(ACTION_STOP)
        val stopPendingIntent = PendingIntent.getService(
            this,
            1,
            stopIntent,
            PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT,
        )
        val openAppIntent = Intent(this, MainActivity::class.java)
            .addFlags(Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_CLEAR_TOP or Intent.FLAG_ACTIVITY_SINGLE_TOP)
        val openAppPendingIntent = PendingIntent.getActivity(
            this,
            2,
            openAppIntent,
            PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT,
        )
        val builder = Notification.Builder(this, NOTIFICATION_CHANNEL_ID)
            .setSmallIcon(android.R.drawable.stat_notify_sync)
            .setContentTitle(title)
            .setContentText(text)
            .setContentIntent(openAppPendingIntent)
            .setOngoing(ongoing)
            .addAction(android.R.drawable.ic_menu_close_clear_cancel, "Stop", stopPendingIntent)
        if (!bigText.isNullOrBlank()) {
            builder.setStyle(Notification.BigTextStyle().bigText(bigText))
        }
        return builder.build()
    }

    private fun createNotificationChannel() {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.O) {
            return
        }
        val manager = getSystemService(NotificationManager::class.java)
        if (manager.getNotificationChannel(NOTIFICATION_CHANNEL_ID) != null) {
            return
        }
        val channel = NotificationChannel(
            NOTIFICATION_CHANNEL_ID,
            "Thruflux engine",
            NotificationManager.IMPORTANCE_LOW,
        )
        manager.createNotificationChannel(channel)
    }

    companion object {
        const val ACTION_STOP = "app.thruflux.android.action.STOP_ENGINE"
        const val ACTION_UPDATE_TRANSFER = "app.thruflux.android.action.UPDATE_TRANSFER_NOTIFICATION"
        const val ACTION_CLEAR_TRANSFER = "app.thruflux.android.action.CLEAR_TRANSFER_NOTIFICATION"
        const val EXTRA_NOTIFICATION_TITLE = "notification_title"
        const val EXTRA_NOTIFICATION_TEXT = "notification_text"
        const val EXTRA_NOTIFICATION_BIG_TEXT = "notification_big_text"
        private const val SERVICE_TAG = "ThrufluxEngineService"
        private const val ENGINE_TAG = "ThrufluxEngine"
        private const val NOTIFICATION_ID = 1001
        private const val NOTIFICATION_CHANNEL_ID = "thruflux_engine"
        private const val ENGINE_BINARY_NAME = "libthru_android.so"
        private const val ENGINE_START_TIMEOUT_MS = 20_000L
        private const val ENGINE_POLL_INTERVAL_MS = 400L
        private const val ENGINE_HEALTH_TIMEOUT_MS = 1_200
    }
}

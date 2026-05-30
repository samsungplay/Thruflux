package app.thruflux.android

import android.Manifest
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.graphics.BitmapFactory
import android.content.ClipData
import android.content.ClipboardManager
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.media.MediaMetadataRetriever
import android.net.Uri
import android.os.Build
import android.os.Bundle
import android.os.Environment
import android.os.Handler
import android.os.Looper
import android.provider.Settings as AndroidSettings
import androidx.activity.ComponentActivity
import androidx.activity.compose.rememberLauncherForActivityResult
import androidx.activity.compose.setContent
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.foundation.background
import androidx.compose.foundation.border
import androidx.compose.foundation.Image
import androidx.compose.foundation.clickable
import androidx.compose.foundation.horizontalScroll
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.foundation.verticalScroll
import androidx.compose.animation.core.LinearEasing
import androidx.compose.animation.core.RepeatMode
import androidx.compose.animation.core.animateFloat
import androidx.compose.animation.core.infiniteRepeatable
import androidx.compose.animation.core.rememberInfiniteTransition
import androidx.compose.animation.core.tween
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.BoxWithConstraints
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.ColumnScope
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.fillMaxHeight
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.safeDrawingPadding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.aspectRatio
import androidx.compose.foundation.layout.widthIn
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.rounded.ArrowBack
import androidx.compose.material.icons.rounded.Bolt
import androidx.compose.material.icons.rounded.CameraAlt
import androidx.compose.material.icons.rounded.Computer
import androidx.compose.material.icons.rounded.ContentCopy
import androidx.compose.material.icons.rounded.DarkMode
import androidx.compose.material.icons.rounded.Delete
import androidx.compose.material.icons.rounded.Download
import androidx.compose.material.icons.rounded.InsertDriveFile
import androidx.compose.material.icons.rounded.Folder
import androidx.compose.material.icons.rounded.Inbox
import androidx.compose.material.icons.rounded.Info
import androidx.compose.material.icons.rounded.LightMode
import androidx.compose.material.icons.rounded.Lock
import androidx.compose.material.icons.rounded.Refresh
import androidx.compose.material.icons.rounded.Settings
import androidx.compose.material.icons.rounded.Share
import androidx.compose.material.icons.rounded.Stop
import androidx.compose.material.icons.rounded.Upload
import androidx.compose.material3.Button
import androidx.compose.material3.Icon
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.OutlinedTextFieldDefaults
import androidx.compose.material3.Snackbar
import androidx.compose.material3.SnackbarHost
import androidx.compose.material3.SnackbarHostState
import androidx.compose.material3.Slider
import androidx.compose.material3.Surface
import androidx.compose.material3.Switch
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.layout.ContentScale
import androidx.compose.ui.draw.rotate
import androidx.compose.ui.graphics.Brush
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.asImageBitmap
import androidx.compose.ui.graphics.vector.ImageVector
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.res.painterResource
import androidx.compose.ui.text.ExperimentalTextApi
import androidx.compose.ui.text.font.Font
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.font.FontVariation
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.tooling.preview.Preview
import androidx.compose.ui.text.input.KeyboardType
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.compose.ui.window.Dialog
import androidx.compose.material3.Typography
import org.json.JSONArray
import org.json.JSONObject
import java.io.File
import java.net.HttpURLConnection
import java.net.URL
import java.security.SecureRandom
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlin.math.roundToInt
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.sse.EventSource
import okhttp3.sse.EventSourceListener
import okhttp3.sse.EventSources

class MainActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContent {
            ThrufluxApp()
        }
    }
}

@Composable
private fun ThrufluxApp() {
    MaterialTheme(typography = thrufluxTypography()) {
        Surface(modifier = Modifier.fillMaxSize(), color = Color.Transparent) {
            val context = LocalContext.current
            var screen by remember { mutableStateOf(AppScreen.Home) }
            var showSplash by remember { mutableStateOf(true) }
            var showEnginePrompt by remember { mutableStateOf(false) }
            var themePreference by remember { mutableStateOf(loadThemePreference(context)) }
            var deviceCode by remember { mutableStateOf(loadDeviceCode(context)) }
            var settings by remember { mutableStateOf(loadAppSettings(context)) }
            var sendEntries by remember { mutableStateOf<List<SendEntry>>(emptyList()) }
            var sendFlowStage by remember { mutableStateOf(SendFlowStage.Idle) }
            var sendJoinCode by remember { mutableStateOf("") }
            var isStartingSend by remember { mutableStateOf(false) }
            var manifestProgress by remember { mutableStateOf(ManifestProgress()) }
            var senderTransfers by remember { mutableStateOf<List<SenderTransferProgress>>(emptyList()) }
            var showFilePicker by remember { mutableStateOf(false) }
            var showReceiveDirectoryPicker by remember { mutableStateOf(false) }
            var appDialog by remember { mutableStateOf<AppDialog?>(null) }
            var showSaveDeviceDialog by remember { mutableStateOf(false) }
            var saveDeviceName by remember { mutableStateOf("") }
            var senderEventThread by remember { mutableStateOf<Thread?>(null) }
            var receiveJoinCode by remember { mutableStateOf("") }
            var receiveSaveDirectory by remember { mutableStateOf(defaultReceiveDirectory()) }
            var receiveFlowStage by remember { mutableStateOf(ReceiveFlowStage.Idle) }
            var isStartingReceive by remember { mutableStateOf(false) }
            var receiveManifestTotalSize by remember { mutableStateOf(0L) }
            var receiveManifestSummaryFilesCount by remember { mutableStateOf(0) }
            var receiveManifestSummaryTotalSize by remember { mutableStateOf(0L) }
            var receiveTransferProgress by remember { mutableStateOf(ReceiveTransferProgress()) }
            var savedDevices by remember { mutableStateOf(loadSavedDevices(context)) }
            var receiverTransferFailed by remember { mutableStateOf(false) }
            var receiverCompleteNotified by remember { mutableStateOf(false) }
            var receiverFailedNotified by remember { mutableStateOf(false) }
            val notifiedJoined = remember { mutableSetOf<String>() }
            val notifiedCompleted = remember { mutableSetOf<String>() }
            val notifiedFailed = remember { mutableSetOf<String>() }
            val mainHandler = remember { Handler(Looper.getMainLooper()) }
            val snackbarHostState = remember { SnackbarHostState() }
            val snackbarScope = rememberCoroutineScope()
            val engineState by EngineStatus.state.collectAsState()
            val engineBaseUrl by EngineStatus.baseUrl.collectAsState()
            val notificationPermissionLauncher = rememberLauncherForActivityResult(
                ActivityResultContracts.RequestPermission(),
            ) { granted ->
                if (granted) {
                    startEngineService(context)
                }
            }
            val readPermissionLauncher = rememberLauncherForActivityResult(
                ActivityResultContracts.RequestMultiplePermissions(),
            ) {}
            val storageSettingsLauncher = rememberLauncherForActivityResult(
                ActivityResultContracts.StartActivityForResult(),
            ) {
                if (!hasStorageAccess(context)) {
                    appDialog = AppDialog(
                        title = "Storage access needed",
                        message = "Thruflux still does not have all files access. Open Settings again when you are ready to send files.",
                    )
                }
            }
            fun requestEngineStart() {
                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU &&
                    context.checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED
                ) {
                    notificationPermissionLauncher.launch(Manifest.permission.POST_NOTIFICATIONS)
                } else {
                    startEngineService(context)
                }
            }
            fun requestStorageAccess() {
                if (hasStorageAccess(context)) {
                    return
                }
                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.R) {
                    appDialog = AppDialog(
                        title = "Storage access needed",
                        message = "Thruflux needs all files access to browse and send files by path.",
                        actionLabel = "Open settings",
                        onAction = { openManageStorageSettings(context) { intent -> storageSettingsLauncher.launch(intent) } },
                    )
                } else {
                    readPermissionLauncher.launch(arrayOf(Manifest.permission.READ_EXTERNAL_STORAGE))
                }
            }
            fun copyWithSnackbar(label: String, text: String) {
                copyText(context, label, text)
                snackbarScope.launch {
                    snackbarHostState.currentSnackbarData?.dismiss()
                    snackbarHostState.showSnackbar("$label copied")
                }
            }
            fun resetSenderState() {
                sendFlowStage = SendFlowStage.Idle
                sendJoinCode = ""
                isStartingSend = false
                manifestProgress = ManifestProgress()
                senderTransfers = emptyList()
                senderEventThread?.interrupt()
                senderEventThread = null
                notifiedJoined.clear()
                notifiedCompleted.clear()
                notifiedFailed.clear()
                clearForegroundTransferNotification(context)
            }
            fun resetReceiveState() {
                receiveFlowStage = ReceiveFlowStage.Idle
                isStartingReceive = false
                receiveManifestTotalSize = 0L
                receiveManifestSummaryFilesCount = 0
                receiveManifestSummaryTotalSize = 0L
                receiveTransferProgress = ReceiveTransferProgress()
                receiverTransferFailed = false
                receiverCompleteNotified = false
                receiverFailedNotified = false
                senderEventThread?.interrupt()
                senderEventThread = null
                clearForegroundTransferNotification(context)
            }
            fun startSenderEvents(baseUrl: String) {
                senderEventThread?.interrupt()
                val thread = subscribeSenderEvents(
                    baseUrl = baseUrl,
                    onEvent = { event ->
                        mainHandler.post {
                            when (event.type) {
                                "manifest_build_start" -> {
                                    sendFlowStage = SendFlowStage.ManifestBuilding
                                    manifestProgress = ManifestProgress()
                                }
                                "manifest_build_progress" -> {
                                    sendFlowStage = SendFlowStage.ManifestBuilding
                                    manifestProgress = ManifestProgress(
                                        filesCount = event.message.optInt("files_count", 0),
                                        totalSize = event.message.optLong("total_size", 0L),
                                    )
                                }
                                "manifest_encoding" -> {
                                    sendFlowStage = SendFlowStage.ManifestEncoding
                                    manifestProgress = manifestProgress.copy(percent = 92)
                                }
                                "manifest_sealed" -> {
                                    sendFlowStage = SendFlowStage.ManifestSealed
                                    manifestProgress = manifestProgress.copy(
                                        filesCount = event.message.optInt("files_count", manifestProgress.filesCount),
                                        totalSize = event.message.optLong("total_size", manifestProgress.totalSize),
                                        percent = 100,
                                    )
                                }
                                "connecting" -> sendFlowStage = SendFlowStage.Connecting
                                "connect_success" -> sendFlowStage = SendFlowStage.Connected
                                "join_code_issued" -> {
                                    sendJoinCode = event.message.optString("join_code", "")
                                    sendFlowStage = SendFlowStage.CodeReady
                                }
                                "connect_error" -> {
                                    sendFlowStage = SendFlowStage.Idle
                                    appDialog = AppDialog("Connection issue", event.message.optString("reason", "Could not connect"))
                                }
                                "disconnected" -> {
                                    val reason = event.message.optString("reason", "")
                                    if (!isNormalClosure(reason)) {
                                        resetSenderState()
                                        appDialog = AppDialog("Connection ended", reason.ifBlank { "Connection ended" })
                                    }
                                }
                                "progress" -> {
                                    val receiverId = senderReceiverId(event.message, senderTransfers) ?: return@post
                                    val hasError = event.message.optBooleanAny(listOf("hasError", "has_error"), false)
                                    val percent = event.message.optDoubleAny(listOf("percent"), 0.0)
                                    val status = when {
                                        hasError -> TransferStatus.Failed
                                        percent >= 100.0 -> TransferStatus.Completed
                                        else -> TransferStatus.Ongoing
                                    }
                                    val next = SenderTransferProgress(
                                        receiverId = receiverId,
                                        ewmaThroughput = event.message.optDoubleAny(listOf("ewmaThroughput", "ewma_throughput"), 0.0),
                                        bytesMoved = event.message.optLongAny(listOf("bytesMoved", "bytes_moved"), 0L),
                                        skippedBytes = event.message.optLongAny(listOf("skippedBytes", "skipped_bytes"), 0L),
                                        filesMoved = event.message.optIntAny(listOf("filesMoved", "files_moved"), 0),
                                        totalExpectedFilesCount = event.message.optIntAny(listOf("totalExpectedFilesCount", "total_expected_files_count"), 0),
                                        isRelayed = event.message.optBooleanAny(listOf("isRelayed", "is_relayed"), false),
                                        percent = percent,
                                        hasError = hasError,
                                        status = status,
                                    )
                                    val existed = senderTransfers.any { it.receiverId == receiverId }
                                    val nextTransfers = senderTransfers.filterNot { it.receiverId == receiverId } + next
                                    senderTransfers = nextTransfers
                                    if (nextTransfers.any { it.status == TransferStatus.Ongoing }) {
                                        updateForegroundTransferNotification(
                                            context = context,
                                            title = "Sending files",
                                            text = senderNotificationText(nextTransfers),
                                            bigText = senderNotificationBigText(nextTransfers),
                                        )
                                    } else {
                                        clearForegroundTransferNotification(context)
                                    }
                                    if (!existed && settings.notifySenderReceiverJoined && notifiedJoined.add(receiverId)) {
                                        showTransferNotification(context, "New receiver connected", "Receiver joined: $receiverId")
                                    }
                                    if (status == TransferStatus.Completed && settings.notifySenderReceiverComplete && notifiedCompleted.add(receiverId)) {
                                        showTransferNotification(context, "Receiver finished", "Receiver completed transfer: $receiverId")
                                    }
                                    if (status == TransferStatus.Failed && settings.notifyTransferFailure && notifiedFailed.add(receiverId)) {
                                        showTransferNotification(context, "Receiver transfer failed", "Receiver failed transfer: $receiverId")
                                    }
                                }
                            }
                        }
                    },
                    onError = { message ->
                        mainHandler.post {
                            if (sendFlowStage != SendFlowStage.Idle) {
                                appDialog = AppDialog("Connection issue", message)
                            }
                        }
                    },
                )
                senderEventThread = thread
            }
            fun startReceiveEvents(baseUrl: String) {
                senderEventThread?.interrupt()
                val thread = subscribeSenderEvents(
                    baseUrl = baseUrl,
                    onEvent = { event ->
                        mainHandler.post {
                            when (event.type) {
                                "connecting" -> receiveFlowStage = ReceiveFlowStage.Connecting
                                "connect_success" -> receiveFlowStage = ReceiveFlowStage.Connected
                                "joining_session" -> receiveFlowStage = ReceiveFlowStage.JoiningSession
                                "p2p_start" -> receiveFlowStage = ReceiveFlowStage.P2pStart
                                "p2p_success" -> receiveFlowStage = ReceiveFlowStage.P2pSuccess
                                "manifest_receive_progress" -> {
                                    receiveFlowStage = ReceiveFlowStage.ManifestReceiving
                                    receiveManifestTotalSize = event.message.optLong("total_size", 0L)
                                }
                                "manifest_parsing" -> receiveFlowStage = ReceiveFlowStage.ManifestParsing
                                "quic_handshake_success" -> receiveFlowStage = ReceiveFlowStage.QuicReady
                                "manifest_unsealed" -> {
                                    receiveManifestSummaryFilesCount = event.message.optInt("files_count", 0)
                                    receiveManifestSummaryTotalSize = event.message.optLong("total_size", 0L)
                                    receiveFlowStage = ReceiveFlowStage.Transfer
                                    receiveTransferProgress = receiveTransferProgress.copy(
                                        totalExpectedFilesCount = receiveManifestSummaryFilesCount,
                                        percent = 0.0,
                                    )
                                }
                                "progress" -> {
                                    val hasError = event.message.optBoolean("hasError", event.message.optBoolean("has_error", false))
                                    val nextPercent = event.message.optDouble("percent", 0.0)
                                    receiveTransferProgress = ReceiveTransferProgress(
                                        ewmaThroughput = event.message.optDouble("ewmaThroughput", event.message.optDouble("ewma_throughput", 0.0)),
                                        bytesMoved = event.message.optLong("bytesMoved", event.message.optLong("bytes_moved", 0L)),
                                        skippedBytes = event.message.optLong("skippedBytes", event.message.optLong("skipped_bytes", 0L)),
                                        filesMoved = event.message.optInt("filesMoved", event.message.optInt("files_moved", 0)),
                                        totalExpectedFilesCount = event.message.optInt("totalExpectedFilesCount", event.message.optInt("total_expected_files_count", 0)),
                                        isRelayed = event.message.optBoolean("isRelayed", event.message.optBoolean("is_relayed", false)),
                                        percent = if (hasError) nextPercent.coerceAtMost(99.0) else nextPercent,
                                    )
                                    if (!hasError && nextPercent < 100.0) {
                                        updateForegroundTransferNotification(
                                            context = context,
                                            title = "Receiving files",
                                            text = receiverNotificationText(receiveTransferProgress),
                                            bigText = receiverNotificationBigText(receiveTransferProgress),
                                        )
                                    }
                                    if (hasError) {
                                        receiverTransferFailed = true
                                        receiveFlowStage = ReceiveFlowStage.Failed
                                        clearForegroundTransferNotification(context)
                                        if (settings.notifyTransferFailure && !receiverFailedNotified) {
                                            receiverFailedNotified = true
                                            showTransferNotification(context, "Receive failed", "This transfer could not be completed.")
                                        }
                                    }
                                }
                                "receive_complete" -> {
                                    if (!receiverTransferFailed) {
                                        receiveFlowStage = ReceiveFlowStage.Complete
                                        receiveTransferProgress = receiveTransferProgress.copy(percent = 100.0)
                                        clearForegroundTransferNotification(context)
                                        if (settings.notifyReceiverSessionComplete && !receiverCompleteNotified) {
                                            receiverCompleteNotified = true
                                            showTransferNotification(context, "Transfer complete", "Your received files are ready.")
                                        }
                                    }
                                }
                                "connect_error" -> {
                                    resetReceiveState()
                                    appDialog = AppDialog("Connection issue", event.message.optString("reason", "Could not connect"))
                                }
                                "disconnected" -> {
                                    val reason = event.message.optString("reason", "")
                                    if (!isNormalClosure(reason)) {
                                        resetReceiveState()
                                        appDialog = AppDialog("Connection ended", reason.ifBlank { "Connection ended" })
                                    }
                                }
                                "p2p_failed" -> {
                                    resetReceiveState()
                                    appDialog = AppDialog("Could not make a direct link", "Setting up a fast path between devices failed.")
                                }
                                "manifest_receive_error" -> {
                                    appDialog = AppDialog("Could not read the file list", "Code: ${event.message.optString("errno", "unknown")}")
                                }
                                "ice_not_ready" -> {
                                    resetReceiveState()
                                    appDialog = AppDialog("Connection setup is not ready", "Trying to connect to your sender")
                                }
                            }
                        }
                    },
                    onError = { message ->
                        mainHandler.post {
                            if (
                                receiveFlowStage != ReceiveFlowStage.Idle &&
                                receiveFlowStage != ReceiveFlowStage.Complete &&
                                receiveFlowStage != ReceiveFlowStage.Failed
                            ) {
                                appDialog = AppDialog("Connection issue", message)
                            }
                        }
                    },
                )
                senderEventThread = thread
            }
            fun goHomeFromSend() {
                if (sendFlowStage == SendFlowStage.Idle) {
                    screen = AppScreen.Home
                    return
                }
                val baseUrl = engineBaseUrl
                Thread {
                    if (baseUrl != null) {
                        runCatching { postJson(baseUrl, "/abort", null) }
                    }
                    mainHandler.post {
                        resetSenderState()
                        screen = AppScreen.Home
                    }
                }.start()
            }
            fun startSending() {
                val baseUrl = engineBaseUrl
                val errors = validateSettings(settings)
                when {
                    isStartingSend -> return
                    sendEntries.isEmpty() -> return
                    baseUrl == null || engineState != EngineState.Ready -> {
                        appDialog = AppDialog("Engine not ready", "Start the Thruflux engine before sending files.")
                        return
                    }
                    !hasStorageAccess(context) -> {
                        appDialog = AppDialog(
                            title = "Storage access needed",
                            message = "Thruflux needs all files access before it can send files.",
                            actionLabel = "Open settings",
                            onAction = { openManageStorageSettings(context) { intent -> storageSettingsLauncher.launch(intent) } },
                        )
                        return
                    }
                    !errors.isValid -> {
                        appDialog = AppDialog("Settings need attention", "Fix invalid settings before sending files.")
                        return
                    }
                }
                isStartingSend = true
                sendJoinCode = ""
                senderTransfers = emptyList()
                manifestProgress = ManifestProgress()
                sendFlowStage = SendFlowStage.Starting
                notifiedJoined.clear()
                notifiedCompleted.clear()
                notifiedFailed.clear()
                startSenderEvents(baseUrl)
                Thread {
                    val result = runCatching {
                        val payload = buildHostPayload(sendEntries, settings, deviceCode)
                        postJson(baseUrl, "/host", payload)
                    }
                    mainHandler.post {
                        isStartingSend = false
                        result.exceptionOrNull()?.let {
                            sendFlowStage = SendFlowStage.Idle
                            appDialog = AppDialog("Could not start sending", it.message ?: "Could not start sending")
                        }
                    }
                }.start()
            }
            fun goHomeFromReceive() {
                if (receiveFlowStage == ReceiveFlowStage.Idle) {
                    screen = AppScreen.Home
                    return
                }
                val baseUrl = engineBaseUrl
                Thread {
                    if (baseUrl != null) {
                        runCatching { postJson(baseUrl, "/abort", null) }
                    }
                    mainHandler.post {
                        resetReceiveState()
                        screen = AppScreen.Home
                    }
                }.start()
            }
            fun startReceiving() {
                val baseUrl = engineBaseUrl
                val errors = validateSettings(settings)
                when {
                    isStartingReceive -> return
                    baseUrl == null || engineState != EngineState.Ready -> {
                        appDialog = AppDialog("Engine not ready", "Start the Thruflux engine before receiving files.")
                        return
                    }
                    !isValidJoinCode(receiveJoinCode) -> {
                        appDialog = AppDialog("Could not start receiving", "Enter a valid join code first.")
                        return
                    }
                    receiveSaveDirectory.isBlank() -> {
                        appDialog = AppDialog("Could not start receiving", "Choose where received files will be saved.")
                        return
                    }
                    !hasStorageAccess(context) -> {
                        appDialog = AppDialog(
                            title = "Storage access needed",
                            message = "Thruflux needs all files access before it can save received files.",
                            actionLabel = "Open settings",
                            onAction = { openManageStorageSettings(context) { intent -> storageSettingsLauncher.launch(intent) } },
                        )
                        return
                    }
                    !errors.isValid -> {
                        appDialog = AppDialog("Settings need attention", "Fix invalid settings before receiving files.")
                        return
                    }
                }
                isStartingReceive = true
                receiverTransferFailed = false
                receiverCompleteNotified = false
                receiverFailedNotified = false
                receiveManifestTotalSize = 0L
                receiveManifestSummaryFilesCount = 0
                receiveManifestSummaryTotalSize = 0L
                receiveTransferProgress = ReceiveTransferProgress()
                receiveFlowStage = ReceiveFlowStage.Starting
                startReceiveEvents(baseUrl)
                Thread {
                    val result = runCatching {
                        postJson(baseUrl, "/receive", buildReceivePayload(receiveJoinCode, receiveSaveDirectory, settings))
                    }
                    mainHandler.post {
                        isStartingReceive = false
                        result.exceptionOrNull()?.let {
                            resetReceiveState()
                            appDialog = AppDialog("Could not start receiving", it.message ?: "Could not start receiving")
                        }
                    }
                }.start()
            }
            fun abortReceiver(receiverId: String) {
                val baseUrl = engineBaseUrl ?: return
                Thread {
                    val result = runCatching {
                        postJson(baseUrl, "/abortReceiver", JSONObject().put("receiverId", receiverId))
                    }
                    mainHandler.post {
                        if (result.isSuccess) {
                            senderTransfers = senderTransfers.map {
                                if (it.receiverId == receiverId) it.copy(hasError = true, status = TransferStatus.Failed) else it
                            }
                        } else {
                            appDialog = AppDialog("Could not stop receiver", result.exceptionOrNull()?.message ?: "Could not stop receiver")
                        }
                    }
                }.start()
            }
            LaunchedEffect(Unit) {
                delay(1200)
                showSplash = false
            }
            LaunchedEffect(Unit) {
                if (engineState != EngineState.Ready && engineState != EngineState.Starting) {
                    showEnginePrompt = true
                }
                requestStorageAccess()
            }
            DisposableEffect(Unit) {
                onDispose {
                    senderEventThread?.interrupt()
                }
            }
            Box(modifier = Modifier.fillMaxSize()) {
            when (screen) {
                AppScreen.Home -> HomeScreen(
                    engineState = engineState,
                    themePreference = themePreference,
                    deviceCode = deviceCode,
                    randomJoinCodeMode = settings.randomJoinCodeMode,
                    onSend = { screen = AppScreen.Send },
                    onReceive = { screen = AppScreen.Receive },
                    onBlockedNavigation = { showEnginePrompt = true },
                    onToggleTheme = {
                        themePreference = if (themePreference == ThemePreference.Light) {
                            ThemePreference.Dark
                        } else {
                            ThemePreference.Light
                        }
                        saveThemePreference(context, themePreference)
                    },
                    onCopyDeviceCode = { copyWithSnackbar("Device code", deviceCode) },
                    onShareDeviceCode = { shareText(context, deviceCode) },
                    onRegenerateDeviceCode = {
                        deviceCode = generateDeviceCode()
                        saveDeviceCode(context, deviceCode)
                    },
                    onOpenSettings = { screen = AppScreen.Settings },
                    onOpenDesktopDownload = { openUrl(context, DESKTOP_DOWNLOAD_URL) },
                )
                AppScreen.Send -> SendScreen(
                    themePreference = themePreference,
                    entries = sendEntries,
                    flowStage = sendFlowStage,
                    joinCode = sendJoinCode,
                    isStarting = isStartingSend,
                    manifestProgress = manifestProgress,
                    senderTransfers = senderTransfers,
                    canSend = sendEntries.isNotEmpty() && hasStorageAccess(context) && validateSettings(settings).isValid && engineState == EngineState.Ready,
                    onBack = { goHomeFromSend() },
                    onPick = {
                        if (hasStorageAccess(context)) {
                            showFilePicker = true
                        } else {
                            appDialog = AppDialog(
                                title = "Storage access needed",
                                message = "Thruflux needs all files access before it can browse files.",
                                actionLabel = "Open settings",
                                onAction = { openManageStorageSettings(context) { intent -> storageSettingsLauncher.launch(intent) } },
                            )
                        }
                    },
                    onRemove = { entry -> sendEntries = sendEntries.filterNot { it == entry } },
                    onConfirm = { startSending() },
                    onAbort = { goHomeFromSend() },
                    onShareJoinCode = { if (sendJoinCode.isNotBlank()) shareText(context, sendJoinCode) },
                    onCopyJoinCode = { if (sendJoinCode.isNotBlank()) copyWithSnackbar("Join code", sendJoinCode) },
                    onAbortReceiver = { receiverId -> abortReceiver(receiverId) },
                )
                AppScreen.Receive -> ReceiveScreen(
                    themePreference = themePreference,
                    joinCode = receiveJoinCode,
                    saveDirectory = receiveSaveDirectory,
                    overwrite = settings.overwrite,
                    flowStage = receiveFlowStage,
                    manifestTotalSize = receiveManifestTotalSize,
                    manifestSummaryFilesCount = receiveManifestSummaryFilesCount,
                    manifestSummaryTotalSize = receiveManifestSummaryTotalSize,
                    transferProgress = receiveTransferProgress,
                    savedDevices = savedDevices,
                    canReceive = engineState == EngineState.Ready && isValidJoinCode(receiveJoinCode) && receiveSaveDirectory.isNotBlank() && validateSettings(settings).isValid && !isStartingReceive,
                    onBack = { goHomeFromReceive() },
                    onJoinCodeChange = { receiveJoinCode = it },
                    onSelectSavedDevice = { receiveJoinCode = it.joinCode },
                    onSaveCurrentDevice = {
                        if (!isValidJoinCode(receiveJoinCode)) {
                            appDialog = AppDialog("Could not save device", "Enter a valid join code first.")
                        } else {
                            saveDeviceName = ""
                            showSaveDeviceDialog = true
                        }
                    },
                    onRemoveSavedDevice = { id ->
                        savedDevices = savedDevices.filterNot { it.id == id }
                        saveSavedDevices(context, savedDevices)
                    },
                    onSelectDirectory = {
                        if (hasStorageAccess(context)) {
                            showReceiveDirectoryPicker = true
                        } else {
                            appDialog = AppDialog(
                                title = "Storage access needed",
                                message = "Thruflux needs all files access before it can choose a save folder.",
                                actionLabel = "Open settings",
                                onAction = { openManageStorageSettings(context) { intent -> storageSettingsLauncher.launch(intent) } },
                            )
                        }
                    },
                    onOverwriteChange = { value ->
                        settings = settings.copy(overwrite = value)
                        saveAppSettings(context, settings)
                    },
                    onReceive = { startReceiving() },
                    onAbort = { goHomeFromReceive() },
                    onCopySavePath = { copyWithSnackbar("Save path", receiveSaveDirectory) },
                    onRetry = { resetReceiveState() },
                )
                AppScreen.Settings -> SettingsShell(
                    themePreference = themePreference,
                    settings = settings,
                    onPatchSettings = { patch ->
                        settings = patch(settings)
                        saveAppSettings(context, settings)
                    },
                    onRestoreDefaults = {
                        settings = AppSettings()
                        saveAppSettings(context, settings)
                    },
                    onToggleTheme = {
                        themePreference = if (themePreference == ThemePreference.Light) {
                            ThemePreference.Dark
                        } else {
                            ThemePreference.Light
                        }
                        saveThemePreference(context, themePreference)
                    },
                    onOpenPrivacyPolicy = { openUrl(context, PRIVACY_POLICY_URL) },
                    onBack = { screen = AppScreen.Home },
                )
            }
            if (!showSplash && showEnginePrompt && appDialog == null && !showFilePicker && engineState != EngineState.Ready && engineState != EngineState.Starting) {
                ThemedModal(
                    palette = palette(themePreference),
                    onDismissRequest = { showEnginePrompt = false },
                    title = "Start Thruflux engine",
                    body = {
                        Text(
                            "Start the local engine so this device can send and receive files.",
                            color = palette(themePreference).textSoft,
                            fontSize = 14.sp,
                            lineHeight = 19.sp,
                        )
                    },
                    actions = {
                        TextButton(onClick = { showEnginePrompt = false }) {
                            Text("Not now")
                        }
                        Button(
                            onClick = {
                                showEnginePrompt = false
                                requestEngineStart()
                            },
                        ) {
                            Text("Start")
                        }
                    },
                )
            }
            if (!showSplash) appDialog?.let { dialog ->
                val modalPalette = palette(themePreference)
                ThemedModal(
                    palette = modalPalette,
                    onDismissRequest = { appDialog = null },
                    title = dialog.title,
                    body = {
                        Text(
                            dialog.message,
                            color = modalPalette.textSoft,
                            fontSize = 14.sp,
                            lineHeight = 19.sp,
                        )
                    },
                    actions = {
                        if (dialog.actionLabel != null) {
                            TextButton(onClick = { appDialog = null }) {
                                Text("Cancel")
                            }
                        }
                        if (dialog.actionLabel != null && dialog.onAction != null) {
                            Button(
                                onClick = {
                                    val action = dialog.onAction
                                    appDialog = null
                                    action()
                                },
                            ) {
                                Text(dialog.actionLabel)
                            }
                        } else {
                            Button(onClick = { appDialog = null }) {
                                Text("OK")
                            }
                        }
                    },
                )
            }
            if (showFilePicker) {
                FilePickerDialog(
                    themePreference = themePreference,
                    directoryOnly = false,
                    pickDirectoriesImmediately = false,
                    onDismiss = { showFilePicker = false },
                    onPick = { entries ->
                        sendEntries = mergeSendEntries(sendEntries, entries)
                        showFilePicker = false
                    },
                    onError = { message -> appDialog = AppDialog("Could not read folder", message) },
                )
            }
            if (showReceiveDirectoryPicker) {
                FilePickerDialog(
                    themePreference = themePreference,
                    directoryOnly = true,
                    pickDirectoriesImmediately = true,
                    onDismiss = { showReceiveDirectoryPicker = false },
                    onPick = { entries ->
                        entries.firstOrNull { it.isDirectory }?.let {
                            receiveSaveDirectory = it.path
                        }
                        showReceiveDirectoryPicker = false
                    },
                    onError = { message -> appDialog = AppDialog("Could not read folder", message) },
                )
            }
            if (showSaveDeviceDialog) {
                val modalPalette = palette(themePreference)
                ThemedModal(
                    palette = modalPalette,
                    onDismissRequest = { showSaveDeviceDialog = false },
                    title = "Save device",
                    body = {
                        OutlinedTextField(
                            value = saveDeviceName,
                            onValueChange = { saveDeviceName = it },
                            label = { Text("Device name") },
                            placeholder = { Text("Saved device") },
                            singleLine = true,
                            colors = themedTextFieldColors(modalPalette),
                            modifier = Modifier.fillMaxWidth(),
                        )
                    },
                    actions = {
                        TextButton(onClick = { showSaveDeviceDialog = false }) {
                            Text("Cancel")
                        }
                        Button(
                            onClick = {
                                val now = System.currentTimeMillis()
                                val code = normalizeJoinCode(receiveJoinCode)
                                val name = saveDeviceName.trim().ifBlank { "Saved device" }
                                val existing = savedDevices.firstOrNull { it.joinCode.equals(code, ignoreCase = true) }
                                val next = if (existing != null) {
                                    savedDevices.map {
                                        if (it.id == existing.id) it.copy(name = name, joinCode = code, updatedAt = now) else it
                                    }
                                } else {
                                    listOf(SavedDevice(now.toString(), name, code, now, now)) + savedDevices
                                }.sortedByDescending { it.updatedAt }
                                savedDevices = next
                                saveSavedDevices(context, next)
                                showSaveDeviceDialog = false
                            },
                        ) {
                            Text("Save")
                        }
                    },
                )
            }
            if (showSplash) {
                SplashOverlay(themePreference = themePreference)
            }
            SnackbarHost(
                hostState = snackbarHostState,
                modifier = Modifier
                    .align(Alignment.BottomCenter)
                    .safeDrawingPadding()
                    .padding(16.dp),
            ) { data ->
                Snackbar(
                    containerColor = palette(themePreference).surfaceStrong,
                    contentColor = palette(themePreference).text,
                    actionColor = Color(0xFF1976FF),
                    snackbarData = data,
                )
            }
            }
        }
    }
}

private enum class AppScreen {
    Home,
    Send,
    Receive,
    Settings,
}

private enum class ThemePreference {
    Light,
    Dark,
}

private data class AppSettings(
    val serverUrl: String = DEFAULT_SERVER_URL,
    val stunServer: String = DEFAULT_STUN_SERVER,
    val turnServers: String = "",
    val forceTurn: Boolean = false,
    val quicConnWindowBytes: Long = DEFAULT_QCW,
    val quicStreamWindowBytes: Long = DEFAULT_QSW,
    val overwrite: Boolean = false,
    val udpBufferBytes: Long = DEFAULT_UDP,
    val maxReceivers: Int = DEFAULT_MAX_RECEIVERS,
    val randomJoinCodeMode: Boolean = false,
    val notifyReceiverSessionComplete: Boolean = true,
    val notifySenderReceiverJoined: Boolean = true,
    val notifySenderReceiverComplete: Boolean = true,
    val notifyTransferFailure: Boolean = true,
)

private data class SettingsErrors(
    val serverUrl: String?,
    val stunServer: String?,
    val turnServers: String?,
    val quicRelation: String?,
)

private val SettingsErrors.isValid: Boolean
    get() = serverUrl == null && stunServer == null && turnServers == null && quicRelation == null

private data class SendEntry(
    val path: String,
    val size: Long?,
    val isDirectory: Boolean,
)

private data class PickerEntry(
    val path: String,
    val name: String,
    val size: Long?,
    val isDirectory: Boolean,
    val lastModified: Long,
)

private data class QuickFolder(
    val label: String,
    val path: String,
    val icon: ImageVector,
)

private enum class SendFlowStage {
    Idle,
    Starting,
    ManifestBuilding,
    ManifestEncoding,
    ManifestSealed,
    Connecting,
    Connected,
    CodeReady,
}

private data class ManifestProgress(
    val filesCount: Int = 0,
    val totalSize: Long = 0,
    val percent: Int = 0,
)

private enum class TransferStatus {
    Ongoing,
    Completed,
    Failed,
}

private data class SenderTransferProgress(
    val receiverId: String,
    val ewmaThroughput: Double,
    val bytesMoved: Long,
    val skippedBytes: Long,
    val filesMoved: Int,
    val totalExpectedFilesCount: Int,
    val isRelayed: Boolean,
    val percent: Double,
    val hasError: Boolean,
    val status: TransferStatus,
)

private enum class ReceiveFlowStage {
    Idle,
    Starting,
    Connecting,
    Connected,
    JoiningSession,
    P2pStart,
    P2pSuccess,
    ManifestReceiving,
    ManifestParsing,
    QuicReady,
    Transfer,
    Failed,
    Complete,
}

private data class ReceiveTransferProgress(
    val ewmaThroughput: Double = 0.0,
    val bytesMoved: Long = 0L,
    val skippedBytes: Long = 0L,
    val filesMoved: Int = 0,
    val totalExpectedFilesCount: Int = 0,
    val isRelayed: Boolean = false,
    val percent: Double = 0.0,
)

private data class SavedDevice(
    val id: String,
    val name: String,
    val joinCode: String,
    val createdAt: Long,
    val updatedAt: Long,
)

private data class ThrufluxEvent(
    val type: String,
    val message: JSONObject,
)

private data class AppDialog(
    val title: String,
    val message: String,
    val actionLabel: String? = null,
    val onAction: (() -> Unit)? = null,
)

private data class HomePalette(
    val bg: Color,
    val bgAccent: Color,
    val surface: Color,
    val surfaceStrong: Color,
    val border: Color,
    val text: Color,
    val textSoft: Color,
)

@Composable
private fun SplashOverlay(themePreference: ThemePreference) {
    val palette = palette(themePreference)
    Box(
        modifier = Modifier
            .fillMaxSize()
            .background(Brush.linearGradient(listOf(palette.bg, palette.bgAccent)))
            .safeDrawingPadding()
            .padding(28.dp),
        contentAlignment = Alignment.Center,
    ) {
        Column(
            horizontalAlignment = Alignment.CenterHorizontally,
            verticalArrangement = Arrangement.spacedBy(18.dp),
        ) {
            Box(
                modifier = Modifier
                    .size(116.dp)
                    .clip(RoundedCornerShape(30.dp))
                    .background(palette.surfaceStrong)
                    .border(1.dp, palette.border, RoundedCornerShape(30.dp))
                    .padding(18.dp),
                contentAlignment = Alignment.Center,
            ) {
                Image(
                    painter = painterResource(id = R.drawable.app_icon),
                    contentDescription = null,
                    modifier = Modifier.fillMaxSize(),
                )
            }
            Column(
                horizontalAlignment = Alignment.CenterHorizontally,
                verticalArrangement = Arrangement.spacedBy(8.dp),
            ) {
                Row(
                    verticalAlignment = Alignment.CenterVertically,
                    horizontalArrangement = Arrangement.spacedBy(6.dp),
                ) {
                    Text(
                        text = "Thruflux",
                        color = palette.text,
                        fontSize = 30.sp,
                        fontWeight = FontWeight.ExtraBold,
                    )
                    Icon(
                        imageVector = Icons.Rounded.Bolt,
                        contentDescription = null,
                        tint = Color(0xFFFFC857),
                        modifier = Modifier.size(26.dp),
                    )
                }
                Text(
                    text = "Built to move big files.",
                    color = palette.textSoft,
                    fontSize = 16.sp,
                    fontWeight = FontWeight.Bold,
                    textAlign = TextAlign.Center,
                )
            }
        }
    }
}

@Composable
private fun HomeScreen(
    engineState: EngineState,
    themePreference: ThemePreference,
    deviceCode: String,
    randomJoinCodeMode: Boolean,
    onSend: () -> Unit,
    onReceive: () -> Unit,
    onBlockedNavigation: () -> Unit,
    onToggleTheme: () -> Unit,
    onCopyDeviceCode: () -> Unit,
    onShareDeviceCode: () -> Unit,
    onRegenerateDeviceCode: () -> Unit,
    onOpenSettings: () -> Unit,
    onOpenDesktopDownload: () -> Unit,
) {
    val palette = palette(themePreference)
    BoxWithConstraints(
        modifier = Modifier
            .fillMaxSize()
            .background(Brush.linearGradient(listOf(palette.bg, palette.bgAccent)))
            .safeDrawingPadding()
            .padding(horizontal = 12.dp, vertical = 10.dp),
    ) {
        val compactHeight = maxHeight < 700.dp
        Box(modifier = Modifier.fillMaxSize(), contentAlignment = Alignment.TopCenter) {
            HomeContent(
                modifier = Modifier
                    .widthIn(max = 680.dp)
                    .fillMaxWidth()
                    .fillMaxHeight(),
                scrollable = compactHeight,
                engineState = engineState,
                themePreference = themePreference,
                palette = palette,
                deviceCode = deviceCode,
                randomJoinCodeMode = randomJoinCodeMode,
                onSend = onSend,
                onReceive = onReceive,
                onBlockedNavigation = onBlockedNavigation,
                onToggleTheme = onToggleTheme,
                onCopyDeviceCode = onCopyDeviceCode,
                onShareDeviceCode = onShareDeviceCode,
                onRegenerateDeviceCode = onRegenerateDeviceCode,
                onOpenSettings = onOpenSettings,
                onOpenDesktopDownload = onOpenDesktopDownload,
            )
        }
    }
}

@Composable
private fun HomeContent(
    modifier: Modifier = Modifier,
    scrollable: Boolean,
    engineState: EngineState,
    themePreference: ThemePreference,
    palette: HomePalette,
    deviceCode: String,
    randomJoinCodeMode: Boolean,
    onSend: () -> Unit,
    onReceive: () -> Unit,
    onBlockedNavigation: () -> Unit,
    onToggleTheme: () -> Unit,
    onCopyDeviceCode: () -> Unit,
    onShareDeviceCode: () -> Unit,
    onRegenerateDeviceCode: () -> Unit,
    onOpenSettings: () -> Unit,
    onOpenDesktopDownload: () -> Unit,
) {
    Column(
        modifier = if (scrollable) {
            modifier
                .verticalScroll(rememberScrollState())
        } else {
            modifier
        },
        verticalArrangement = Arrangement.spacedBy(10.dp),
    ) {
        TopBar(
            engineState = engineState,
            themePreference = themePreference,
            palette = palette,
            onToggleTheme = onToggleTheme,
        )
        Column(
            modifier = if (scrollable) Modifier.fillMaxWidth() else Modifier.weight(1f),
            verticalArrangement = Arrangement.spacedBy(10.dp),
        ) {
            ActionCard(
                title = "Send files",
                body = "Share files with one or more receivers",
                icon = Icons.Rounded.Upload,
                brush = Brush.linearGradient(listOf(Color(0xFF0077FF), Color(0xFF35A0FF))),
                modifier = if (scrollable) Modifier.heightIn(min = 170.dp) else Modifier.weight(1f),
                onClick = if (engineState == EngineState.Ready) onSend else onBlockedNavigation,
            )
            ActionCard(
                title = "Receive files",
                body = "Receive files from others with a simple code",
                icon = Icons.Rounded.Download,
                brush = Brush.linearGradient(listOf(Color(0xFF00B894), Color(0xFF25D8B2))),
                modifier = if (scrollable) Modifier.heightIn(min = 170.dp) else Modifier.weight(1f),
                onClick = if (engineState == EngineState.Ready) onReceive else onBlockedNavigation,
            )
        }
        DeviceCodePanel(
            palette = palette,
            deviceCode = deviceCode,
            randomJoinCodeMode = randomJoinCodeMode,
            onCopy = onCopyDeviceCode,
            onShare = onShareDeviceCode,
            onRegenerate = onRegenerateDeviceCode,
            onSettings = onOpenSettings,
        )
        DesktopDownloadHint(palette = palette, onOpen = onOpenDesktopDownload)
        VersionNote(palette = palette)
    }
}

@Composable
private fun TopBar(
    engineState: EngineState,
    themePreference: ThemePreference,
    palette: HomePalette,
    onToggleTheme: () -> Unit,
) {
    val statusText = when (engineState) {
        EngineState.Stopped -> "Not ready"
        EngineState.Starting -> "Checking"
        EngineState.Ready -> "Ready"
        EngineState.Failed -> "Not ready"
    }
    val statusColor = when (engineState) {
        EngineState.Stopped -> Color(0xFFFF4D4F)
        EngineState.Starting -> Color(0xFFFF9F1C)
        EngineState.Ready -> Color(0xFF2CD66F)
        EngineState.Failed -> Color(0xFFFF4D4F)
    }
    Column(
        modifier = Modifier.fillMaxWidth(),
        horizontalAlignment = Alignment.CenterHorizontally,
        verticalArrangement = Arrangement.spacedBy(6.dp),
    ) {
        Column(horizontalAlignment = Alignment.CenterHorizontally) {
            Row(
                verticalAlignment = Alignment.CenterVertically,
                horizontalArrangement = Arrangement.spacedBy(5.dp),
            ) {
                Text(
                    text = "Thruflux",
                    color = palette.text,
                    fontSize = 22.sp,
                    fontWeight = FontWeight.Bold,
                )
                Icon(
                    imageVector = Icons.Rounded.Bolt,
                    contentDescription = null,
                    tint = Color(0xFFFFC857),
                    modifier = Modifier.size(20.dp),
                )
            }
            Text(
                text = "Built for speed. Direct for security. Simple by design.",
                color = palette.textSoft,
                fontSize = 11.sp,
                textAlign = TextAlign.Center,
            )
        }
        Row(
            modifier = Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.SpaceBetween,
            verticalAlignment = Alignment.CenterVertically,
        ) {
            Pill(palette = palette) {
                Row(verticalAlignment = Alignment.CenterVertically) {
                    Box(
                        modifier = Modifier
                            .size(9.dp)
                            .clip(CircleShape)
                            .background(statusColor),
                    )
                    Text(
                        text = "  $statusText",
                        color = palette.text,
                        fontSize = 12.sp,
                        fontWeight = FontWeight.SemiBold,
                    )
                }
            }
            Pill(
                palette = palette,
                modifier = Modifier.clickable(onClick = onToggleTheme),
            ) {
                Row(
                    verticalAlignment = Alignment.CenterVertically,
                    horizontalArrangement = Arrangement.spacedBy(6.dp),
                ) {
                    Icon(
                        imageVector = if (themePreference == ThemePreference.Light) Icons.Rounded.LightMode else Icons.Rounded.DarkMode,
                        contentDescription = null,
                        tint = palette.text,
                        modifier = Modifier.size(15.dp),
                    )
                    Text(
                        text = if (themePreference == ThemePreference.Light) "Light" else "Dark",
                        color = palette.text,
                        fontSize = 12.sp,
                        fontWeight = FontWeight.SemiBold,
                    )
                }
            }
        }
    }
}

@Composable
private fun ActionCard(
    title: String,
    body: String,
    icon: ImageVector,
    brush: Brush,
    modifier: Modifier = Modifier,
    onClick: () -> Unit,
) {
    Column(
        modifier = modifier
            .fillMaxWidth()
            .clip(RoundedCornerShape(24.dp))
            .background(brush)
            .clickable(onClick = onClick)
            .padding(20.dp),
        horizontalAlignment = Alignment.CenterHorizontally,
        verticalArrangement = Arrangement.SpaceBetween,
    ) {
        Text(
            text = title,
            color = Color.White,
            fontSize = 22.sp,
            fontWeight = FontWeight.Bold,
            textAlign = TextAlign.Center,
            modifier = Modifier.fillMaxWidth(),
        )
        Box(
            modifier = Modifier
                .size(78.dp)
                .clip(RoundedCornerShape(22.dp))
                .background(Color.White.copy(alpha = 0.16f))
                .border(1.dp, Color.White.copy(alpha = 0.26f), RoundedCornerShape(22.dp)),
            contentAlignment = Alignment.Center,
        ) {
            Icon(
                imageVector = icon,
                contentDescription = null,
                tint = Color.White,
                modifier = Modifier.size(42.dp),
            )
        }
        Text(
            text = body,
            color = Color.White.copy(alpha = 0.96f),
            fontSize = 14.sp,
            lineHeight = 19.sp,
            textAlign = TextAlign.Center,
            modifier = Modifier.fillMaxWidth(),
        )
    }
}

@Composable
private fun DeviceCodePanel(
    palette: HomePalette,
    deviceCode: String,
    randomJoinCodeMode: Boolean,
    onCopy: () -> Unit,
    onShare: () -> Unit,
    onRegenerate: () -> Unit,
    onSettings: () -> Unit,
) {
    Column(
        modifier = Modifier
            .fillMaxWidth()
            .clip(RoundedCornerShape(16.dp))
            .background(palette.surfaceStrong)
            .border(1.dp, palette.border, RoundedCornerShape(16.dp))
            .padding(14.dp),
        verticalArrangement = Arrangement.spacedBy(10.dp),
    ) {
        Column {
            Text(
                text = "This device code",
                color = palette.text,
                fontSize = 16.sp,
                fontWeight = FontWeight.Bold,
            )
            Text(
                text = "Share this code so receivers can save this device",
                color = palette.textSoft,
                fontSize = 12.sp,
            )
        }
        Row(
            modifier = Modifier.fillMaxWidth(),
            verticalAlignment = Alignment.CenterVertically,
            horizontalArrangement = Arrangement.spacedBy(8.dp),
        ) {
            Text(
                text = if (randomJoinCodeMode) "RANDOM" else deviceCode,
                color = palette.text,
                fontSize = 13.sp,
                fontWeight = FontWeight.ExtraBold,
                textAlign = TextAlign.Center,
                modifier = Modifier
                    .weight(1f)
                    .clip(RoundedCornerShape(12.dp))
                    .background(palette.surface)
                    .border(1.dp, palette.border, RoundedCornerShape(12.dp))
                    .padding(horizontal = 10.dp, vertical = 10.dp),
            )
            IconButtonLike(icon = Icons.Rounded.ContentCopy, palette = palette, enabled = !randomJoinCodeMode, onClick = onCopy)
            IconButtonLike(icon = Icons.Rounded.Share, palette = palette, enabled = !randomJoinCodeMode, onClick = onShare)
            IconButtonLike(icon = Icons.Rounded.Refresh, palette = palette, enabled = !randomJoinCodeMode, onClick = onRegenerate)
            IconButtonLike(icon = Icons.Rounded.Settings, palette = palette, onClick = onSettings)
        }
    }
}

@Composable
private fun DesktopDownloadHint(palette: HomePalette, onOpen: () -> Unit) {
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .clip(RoundedCornerShape(14.dp))
            .background(palette.surfaceStrong)
            .border(1.dp, palette.border, RoundedCornerShape(14.dp))
            .clickable(onClick = onOpen)
            .padding(horizontal = 12.dp, vertical = 10.dp),
        verticalAlignment = Alignment.CenterVertically,
        horizontalArrangement = Arrangement.spacedBy(10.dp),
    ) {
        Icon(
            imageVector = Icons.Rounded.Computer,
            contentDescription = null,
            tint = Color(0xFF1976FF),
            modifier = Modifier.size(18.dp),
        )
        Column(modifier = Modifier.weight(1f)) {
            Text(
                text = "Move files between any devices. Get Thruflux for",
                color = palette.text,
                fontSize = 11.sp,
                fontWeight = FontWeight.Bold,
            )
            Text(
                text = "Mac • Windows • Linux • Android",
                color = palette.textSoft,
                fontSize = 11.sp,
                maxLines = 1,
            )
        }
    }
}

@Composable
private fun IconButtonLike(
    icon: ImageVector,
    palette: HomePalette,
    enabled: Boolean = true,
    onClick: () -> Unit,
) {
    val clickableModifier = if (enabled) {
        Modifier.clickable(onClick = onClick)
    } else {
        Modifier
    }
    Box(
        modifier = Modifier
            .size(38.dp)
            .clip(RoundedCornerShape(10.dp))
            .background(palette.surface)
            .border(1.dp, palette.border, RoundedCornerShape(10.dp))
            .then(clickableModifier),
        contentAlignment = Alignment.Center,
    ) {
        Icon(
            imageVector = icon,
            contentDescription = null,
            tint = if (enabled) palette.text else palette.textSoft,
            modifier = Modifier.size(18.dp),
        )
    }
}

@Composable
private fun ThemedModal(
    palette: HomePalette,
    onDismissRequest: () -> Unit,
    title: String,
    body: @Composable ColumnScope.() -> Unit,
    actions: @Composable () -> Unit,
) {
    Dialog(onDismissRequest = onDismissRequest) {
        Column(
            modifier = Modifier
                .fillMaxWidth()
                .widthIn(max = 560.dp)
                .heightIn(max = 680.dp)
                .clip(RoundedCornerShape(24.dp))
                .background(Brush.linearGradient(listOf(palette.surfaceStrong, palette.surface)))
                .border(1.dp, palette.border, RoundedCornerShape(24.dp))
                .padding(18.dp),
            verticalArrangement = Arrangement.spacedBy(16.dp),
        ) {
            Column(verticalArrangement = Arrangement.spacedBy(6.dp)) {
                Text(
                    text = title,
                    color = palette.text,
                    fontSize = 21.sp,
                    fontWeight = FontWeight.Bold,
                    lineHeight = 25.sp,
                )
                Box(
                    modifier = Modifier
                        .size(width = 42.dp, height = 3.dp)
                        .clip(RoundedCornerShape(999.dp))
                        .background(Color(0xFF1976FF)),
                )
            }
            Column(
                modifier = Modifier
                    .fillMaxWidth()
                    .weight(1f, fill = false)
                    .verticalScroll(rememberScrollState()),
                verticalArrangement = Arrangement.spacedBy(12.dp),
                content = body,
            )
            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.spacedBy(10.dp, Alignment.End),
                verticalAlignment = Alignment.CenterVertically,
            ) {
                actions()
            }
        }
    }
}

@Composable
private fun themedTextFieldColors(palette: HomePalette) = OutlinedTextFieldDefaults.colors(
    focusedTextColor = palette.text,
    unfocusedTextColor = palette.text,
    focusedLabelColor = palette.text,
    unfocusedLabelColor = palette.textSoft,
    focusedBorderColor = palette.text,
    unfocusedBorderColor = palette.border,
    cursorColor = palette.text,
    focusedContainerColor = Color.Transparent,
    unfocusedContainerColor = Color.Transparent,
)

@Composable
private fun SendScreen(
    themePreference: ThemePreference,
    entries: List<SendEntry>,
    flowStage: SendFlowStage,
    joinCode: String,
    isStarting: Boolean,
    manifestProgress: ManifestProgress,
    senderTransfers: List<SenderTransferProgress>,
    canSend: Boolean,
    onBack: () -> Unit,
    onPick: () -> Unit,
    onRemove: (SendEntry) -> Unit,
    onConfirm: () -> Unit,
    onAbort: () -> Unit,
    onShareJoinCode: () -> Unit,
    onCopyJoinCode: () -> Unit,
    onAbortReceiver: (String) -> Unit,
) {
    val palette = palette(themePreference)
    Box(
        modifier = Modifier
            .fillMaxSize()
            .background(Brush.linearGradient(listOf(palette.bg, palette.bgAccent)))
            .safeDrawingPadding()
            .padding(horizontal = 12.dp, vertical = 10.dp),
        contentAlignment = Alignment.TopCenter,
    ) {
        Box(
            modifier = Modifier
                .widthIn(max = 760.dp)
                .fillMaxSize(),
        ) {
            if (flowStage == SendFlowStage.Idle) {
                SendIdleContent(
                    palette = palette,
                    entries = entries,
                    canSend = canSend && !isStarting,
                    isStarting = isStarting,
                    onBack = onBack,
                    onPick = onPick,
                    onRemove = onRemove,
                    onConfirm = onConfirm,
                )
            } else {
                SendStateContent(
                    palette = palette,
                    flowStage = flowStage,
                    joinCode = joinCode,
                    manifestProgress = manifestProgress,
                    senderTransfers = senderTransfers,
                    onBack = onBack,
                    onAbort = onAbort,
                    onShareJoinCode = onShareJoinCode,
                    onCopyJoinCode = onCopyJoinCode,
                    onAbortReceiver = onAbortReceiver,
                )
            }
        }
    }
}

@Composable
private fun SendIdleContent(
    palette: HomePalette,
    entries: List<SendEntry>,
    canSend: Boolean,
    isStarting: Boolean,
    onBack: () -> Unit,
    onPick: () -> Unit,
    onRemove: (SendEntry) -> Unit,
    onConfirm: () -> Unit,
) {
    val fileCount = entries.count { !it.isDirectory }
    val folderCount = entries.count { it.isDirectory }
    Column(
        modifier = Modifier.fillMaxSize(),
        verticalArrangement = Arrangement.spacedBy(12.dp),
    ) {
        SenderTopActions(palette = palette, onBack = onBack, onAbort = null)
        SummaryPillRow(
            firstLabel = "$fileCount files selected",
            firstIcon = Icons.Rounded.InsertDriveFile,
            secondLabel = "$folderCount folders selected",
            secondIcon = Icons.Rounded.Folder,
            palette = palette,
        )
        Column(
            modifier = Modifier
                .fillMaxWidth()
                .weight(1f)
                .clip(RoundedCornerShape(22.dp))
                .background(palette.surfaceStrong)
                .border(1.dp, palette.border, RoundedCornerShape(22.dp))
                .padding(14.dp),
            horizontalAlignment = Alignment.CenterHorizontally,
            verticalArrangement = Arrangement.spacedBy(12.dp),
        ) {
            if (entries.isEmpty()) {
                Column(
                    modifier = Modifier
                        .fillMaxSize()
                        .clickable(onClick = onPick),
                    horizontalAlignment = Alignment.CenterHorizontally,
                    verticalArrangement = Arrangement.Center,
                ) {
                    Box(
                        modifier = Modifier
                            .size(76.dp)
                            .clip(RoundedCornerShape(22.dp))
                            .background(Color(0x221976FF)),
                        contentAlignment = Alignment.Center,
                    ) {
                        Icon(Icons.Rounded.Upload, contentDescription = null, tint = Color(0xFF1976FF), modifier = Modifier.size(38.dp))
                    }
                    Text("Select files to send", color = palette.text, fontSize = 24.sp, fontWeight = FontWeight.Bold, textAlign = TextAlign.Center, modifier = Modifier.padding(top = 12.dp))
                    Text("Choose files or folders from this device", color = palette.textSoft, fontSize = 14.sp, textAlign = TextAlign.Center, modifier = Modifier.padding(top = 6.dp, bottom = 12.dp))
                    Button(onClick = onPick) {
                        Text("Browse files")
                    }
                }
            } else {
                Row(
                    modifier = Modifier.fillMaxWidth(),
                    horizontalArrangement = Arrangement.SpaceBetween,
                    verticalAlignment = Alignment.CenterVertically,
                ) {
                    Text("You can still add more files", color = palette.textSoft, fontSize = 12.sp)
                    Button(onClick = onPick) {
                        Text("Add")
                    }
                }
                Column(
                    verticalArrangement = Arrangement.spacedBy(8.dp),
                    modifier = Modifier
                        .fillMaxWidth()
                        .weight(1f)
                        .verticalScroll(rememberScrollState()),
                ) {
                    entries.forEach { entry ->
                        SendEntryRow(entry = entry, palette = palette, onRemove = { onRemove(entry) })
                    }
                }
            }
        }
        if (entries.isNotEmpty()) {
            Button(
                onClick = onConfirm,
                enabled = canSend,
                modifier = Modifier.fillMaxWidth(),
            ) {
                Text(if (isStarting) "Starting..." else "Send Now")
            }
        }
    }
}

@Composable
private fun SendStateContent(
    palette: HomePalette,
    flowStage: SendFlowStage,
    joinCode: String,
    manifestProgress: ManifestProgress,
    senderTransfers: List<SenderTransferProgress>,
    onBack: () -> Unit,
    onAbort: () -> Unit,
    onShareJoinCode: () -> Unit,
    onCopyJoinCode: () -> Unit,
    onAbortReceiver: (String) -> Unit,
) {
    Column(
        modifier = Modifier.fillMaxSize(),
        verticalArrangement = Arrangement.spacedBy(12.dp),
    ) {
        SenderTopActions(palette = palette, onBack = onBack, onAbort = onAbort)
        Column(
            modifier = Modifier
                .fillMaxWidth()
                .weight(1f)
                .clip(RoundedCornerShape(22.dp))
                .background(palette.surfaceStrong)
                .border(1.dp, palette.border, RoundedCornerShape(22.dp))
                .padding(16.dp),
            horizontalAlignment = Alignment.CenterHorizontally,
            verticalArrangement = if (flowStage == SendFlowStage.CodeReady && senderTransfers.isNotEmpty()) {
                Arrangement.spacedBy(12.dp)
            } else {
                Arrangement.Center
            },
        ) {
            when (flowStage) {
                SendFlowStage.Starting -> StatePanel(
                    icon = Icons.Rounded.Settings,
                    title = "Getting everything ready",
                    body = "Setting up your share request",
                    palette = palette,
                    loading = true,
                )
                SendFlowStage.ManifestBuilding -> {
                    StatePanel(
                        icon = Icons.Rounded.Folder,
                        title = "Preparing your files",
                        body = "Scanning selected files and folders",
                        palette = palette,
                        loading = true,
                    ) {
                        ManifestSummary(manifestProgress, palette)
                    }
                }
                SendFlowStage.ManifestEncoding -> StatePanel(
                    icon = Icons.Rounded.Upload,
                    title = "Finalizing file list",
                    body = "Packaging file details for transfer",
                    palette = palette,
                    loading = true,
                )
                SendFlowStage.ManifestSealed -> {
                    StatePanel(
                        icon = Icons.Rounded.Upload,
                        title = "Ready to connect",
                        body = "File list is complete",
                        palette = palette,
                    ) {
                        ManifestSummary(manifestProgress, palette)
                    }
                }
                SendFlowStage.Connecting -> StatePanel(
                    icon = Icons.Rounded.Upload,
                    title = "Connecting",
                    body = "Trying to reach the session service",
                    palette = palette,
                    loading = true,
                )
                SendFlowStage.Connected -> StatePanel(
                    icon = Icons.Rounded.Upload,
                    title = "Connected",
                    body = "Connection is ready. Getting your share code",
                    palette = palette,
                )
                SendFlowStage.CodeReady -> {
                    if (senderTransfers.isEmpty()) {
                        StatePanel(
                            icon = Icons.Rounded.ContentCopy,
                            title = "Share this code",
                            body = "Share this code and wait for someone to join",
                            palette = palette,
                            loading = joinCode.isBlank(),
                        ) {
                            JoinCodeBox(joinCode, palette, onShareJoinCode, onCopyJoinCode)
                            ManifestSummary(manifestProgress, palette)
                        }
                    } else {
                        Row(
                            modifier = Modifier.fillMaxWidth(),
                            horizontalArrangement = Arrangement.SpaceBetween,
                            verticalAlignment = Alignment.CenterVertically,
                        ) {
                            Column {
                                Text("Share this code", color = palette.text, fontSize = 20.sp, fontWeight = FontWeight.Bold)
                                Text("Active transfers", color = palette.textSoft, fontSize = 13.sp)
                            }
                        }
                        JoinCodeBox(joinCode, palette, onShareJoinCode, onCopyJoinCode)
                        ManifestSummary(manifestProgress, palette)
                        LazyColumn(
                            modifier = Modifier
                                .fillMaxWidth()
                                .weight(1f),
                            verticalArrangement = Arrangement.spacedBy(10.dp),
                        ) {
                            items(senderTransfers, key = { it.receiverId }) { transfer ->
                                SenderTransferRow(transfer = transfer, manifestProgress = manifestProgress, palette = palette, onAbortReceiver = onAbortReceiver)
                            }
                        }
                    }
                }
                SendFlowStage.Idle -> Unit
            }
        }
    }
}

@Composable
private fun SenderTopActions(palette: HomePalette, onBack: () -> Unit, onAbort: (() -> Unit)?) {
    Row(
        modifier = Modifier.fillMaxWidth(),
        horizontalArrangement = Arrangement.SpaceBetween,
        verticalAlignment = Alignment.CenterVertically,
    ) {
        Box(
            modifier = Modifier
                .clip(RoundedCornerShape(12.dp))
                .background(palette.surfaceStrong)
                .border(1.dp, palette.border, RoundedCornerShape(12.dp))
                .clickable(onClick = onBack)
                .padding(horizontal = 12.dp, vertical = 9.dp),
            contentAlignment = Alignment.Center,
        ) {
            Icon(Icons.AutoMirrored.Rounded.ArrowBack, contentDescription = "Back", tint = palette.text, modifier = Modifier.size(20.dp))
        }
        if (onAbort != null) {
            Box(
                modifier = Modifier
                    .clip(RoundedCornerShape(12.dp))
                    .background(Color(0xFFFFE6E6))
                    .border(1.dp, Color(0xFFFFB3B3), RoundedCornerShape(12.dp))
                    .clickable(onClick = onAbort)
                    .padding(horizontal = 12.dp, vertical = 9.dp),
                contentAlignment = Alignment.Center,
            ) {
                Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(6.dp)) {
                    Icon(Icons.Rounded.Stop, contentDescription = null, tint = Color(0xFFB00020), modifier = Modifier.size(18.dp))
                    Text("Stop transfer", color = Color(0xFFB00020), fontSize = 12.sp, fontWeight = FontWeight.SemiBold)
                }
            }
        }
    }
}

@Composable
private fun SummaryPill(label: String, icon: ImageVector, palette: HomePalette, modifier: Modifier = Modifier) {
    Row(
        modifier = modifier
            .clip(RoundedCornerShape(999.dp))
            .background(palette.surfaceStrong)
            .border(1.dp, palette.border, RoundedCornerShape(999.dp))
            .padding(horizontal = 10.dp, vertical = 8.dp),
        verticalAlignment = Alignment.CenterVertically,
        horizontalArrangement = Arrangement.Center,
    ) {
        Icon(icon, contentDescription = null, tint = palette.text, modifier = Modifier.size(16.dp))
        Box(modifier = Modifier.size(6.dp))
        Text(
            text = label,
            color = palette.text,
            fontSize = 12.sp,
            lineHeight = 14.sp,
            fontWeight = FontWeight.SemiBold,
            textAlign = TextAlign.Center,
            modifier = Modifier.weight(1f),
        )
    }
}

@Composable
private fun SummaryPillRow(
    firstLabel: String,
    firstIcon: ImageVector,
    secondLabel: String,
    secondIcon: ImageVector,
    palette: HomePalette,
) {
    BoxWithConstraints(modifier = Modifier.fillMaxWidth()) {
        if (maxWidth < 360.dp) {
            Column(verticalArrangement = Arrangement.spacedBy(8.dp), modifier = Modifier.fillMaxWidth()) {
                SummaryPill(firstLabel, firstIcon, palette, Modifier.fillMaxWidth())
                SummaryPill(secondLabel, secondIcon, palette, Modifier.fillMaxWidth())
            }
        } else {
            Row(horizontalArrangement = Arrangement.spacedBy(8.dp), modifier = Modifier.fillMaxWidth()) {
                SummaryPill(firstLabel, firstIcon, palette, Modifier.weight(1f))
                SummaryPill(secondLabel, secondIcon, palette, Modifier.weight(1f))
            }
        }
    }
}

@Composable
private fun SendEntryRow(entry: SendEntry, palette: HomePalette, onRemove: () -> Unit) {
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .clip(RoundedCornerShape(14.dp))
            .background(palette.surface)
            .border(1.dp, palette.border, RoundedCornerShape(14.dp))
            .padding(10.dp),
        verticalAlignment = Alignment.CenterVertically,
        horizontalArrangement = Arrangement.spacedBy(10.dp),
    ) {
        Icon(if (entry.isDirectory) Icons.Rounded.Folder else Icons.Rounded.InsertDriveFile, contentDescription = null, tint = palette.text, modifier = Modifier.size(22.dp))
        Column(modifier = Modifier.weight(1f)) {
            Text(entry.path, color = palette.text, fontSize = 12.sp, maxLines = 2)
            Text(if (entry.isDirectory) "Folder" else formatSize(entry.size ?: 0L), color = palette.textSoft, fontSize = 11.sp)
        }
        IconButtonLike(icon = Icons.Rounded.Delete, palette = palette, onClick = onRemove)
    }
}

@Composable
private fun StatePanel(
    icon: ImageVector,
    title: String,
    body: String,
    palette: HomePalette,
    loading: Boolean = false,
    content: @Composable ColumnScope.() -> Unit = {},
) {
    Column(
        modifier = Modifier.fillMaxWidth(),
        horizontalAlignment = Alignment.CenterHorizontally,
        verticalArrangement = Arrangement.spacedBy(22.dp),
    ) {
        LoadingIcon(icon = icon, loading = loading)
        Column(
            horizontalAlignment = Alignment.CenterHorizontally,
            verticalArrangement = Arrangement.spacedBy(8.dp),
        ) {
            Text(title, color = palette.text, fontSize = 24.sp, fontWeight = FontWeight.Bold, textAlign = TextAlign.Center)
            Text(body, color = palette.textSoft, fontSize = 14.sp, lineHeight = 19.sp, textAlign = TextAlign.Center)
        }
        Column(
            modifier = Modifier.fillMaxWidth(),
            horizontalAlignment = Alignment.CenterHorizontally,
            verticalArrangement = Arrangement.spacedBy(16.dp),
            content = content,
        )
    }
}

@Composable
private fun LoadingIcon(icon: ImageVector, loading: Boolean) {
    val rotation = if (loading) {
        val transition = rememberInfiniteTransition(label = "sender-loading")
        transition.animateFloat(
            initialValue = 0f,
            targetValue = 360f,
            animationSpec = infiniteRepeatable(
                animation = tween(durationMillis = 1100, easing = LinearEasing),
                repeatMode = RepeatMode.Restart,
            ),
            label = "sender-loading-rotation",
        ).value
    } else {
        0f
    }
    Box(
        modifier = Modifier
            .size(86.dp)
            .clip(RoundedCornerShape(26.dp))
            .background(Color(0x221976FF)),
        contentAlignment = Alignment.Center,
    ) {
        Icon(
            icon,
            contentDescription = null,
            tint = Color(0xFF1976FF),
            modifier = Modifier
                .size(42.dp)
                .rotate(rotation),
        )
    }
}

@Composable
private fun ManifestSummary(progress: ManifestProgress, palette: HomePalette) {
    SummaryPillRow(
        firstLabel = "Items found: ${progress.filesCount}",
        firstIcon = Icons.Rounded.InsertDriveFile,
        secondLabel = "Total size: ${formatSize(progress.totalSize)}",
        secondIcon = Icons.Rounded.Upload,
        palette = palette,
    )
}

@Composable
private fun JoinCodeBox(joinCode: String, palette: HomePalette, onShare: () -> Unit, onCopy: () -> Unit) {
    Column(verticalArrangement = Arrangement.spacedBy(10.dp), horizontalAlignment = Alignment.CenterHorizontally) {
        Text(
            text = joinCode.ifBlank { "Waiting..." },
            color = palette.text,
            fontSize = 24.sp,
            fontWeight = FontWeight.ExtraBold,
            textAlign = TextAlign.Center,
            modifier = Modifier
                .fillMaxWidth()
                .clip(RoundedCornerShape(16.dp))
                .background(palette.surface)
                .border(1.dp, palette.border, RoundedCornerShape(16.dp))
                .padding(14.dp),
        )
        Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
            Button(onClick = onShare, enabled = joinCode.isNotBlank()) {
                Text("Share code")
            }
            Button(onClick = onCopy, enabled = joinCode.isNotBlank()) {
                Text("Copy code")
            }
        }
    }
}

@Composable
private fun SenderTransferRow(
    transfer: SenderTransferProgress,
    manifestProgress: ManifestProgress,
    palette: HomePalette,
    onAbortReceiver: (String) -> Unit,
) {
    Column(
        modifier = Modifier
            .fillMaxWidth()
            .clip(RoundedCornerShape(16.dp))
            .background(palette.surface)
            .border(1.dp, palette.border, RoundedCornerShape(16.dp))
            .padding(12.dp),
        verticalArrangement = Arrangement.spacedBy(8.dp),
    ) {
        Row(
            modifier = Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.SpaceBetween,
            verticalAlignment = Alignment.CenterVertically,
        ) {
            Text("Receiver: ${transfer.receiverId}", color = palette.text, fontSize = 13.sp, fontWeight = FontWeight.Bold, modifier = Modifier.weight(1f))
            Text(
                text = when (transfer.status) {
                    TransferStatus.Ongoing -> "Ongoing"
                    TransferStatus.Completed -> "Completed"
                    TransferStatus.Failed -> "Failed"
                },
                color = when (transfer.status) {
                    TransferStatus.Ongoing -> Color(0xFF1976FF)
                    TransferStatus.Completed -> Color(0xFF00A989)
                    TransferStatus.Failed -> Color(0xFFFF4D4F)
                },
                fontSize = 12.sp,
                fontWeight = FontWeight.SemiBold,
            )
        }
        Text("${transfer.percent.roundToInt().coerceIn(0, 100)}%", color = palette.text, fontSize = 20.sp, fontWeight = FontWeight.Bold)
        SummaryPillRow(
            firstLabel = "Speed: ${formatThroughput(transfer.ewmaThroughput)}",
            firstIcon = Icons.Rounded.Upload,
            secondLabel = "Moved: ${formatSize(transfer.bytesMoved)}",
            secondIcon = Icons.Rounded.Upload,
            palette = palette,
        )
        SummaryPillRow(
            firstLabel = "Skipped: ${formatSize(transfer.skippedBytes)}",
            firstIcon = Icons.Rounded.InsertDriveFile,
            secondLabel = "Files: ${transfer.filesMoved}/${transfer.totalExpectedFilesCount}",
            secondIcon = Icons.Rounded.Folder,
            palette = palette,
        )
        SummaryPillRow(
            firstLabel = "Route: ${if (transfer.isRelayed) "Relayed" else "Direct"}",
            firstIcon = Icons.Rounded.Upload,
            secondLabel = "ETA: ${formatEta(manifestProgress.totalSize, transfer.bytesMoved, transfer.skippedBytes, transfer.ewmaThroughput)}",
            secondIcon = Icons.Rounded.Settings,
            palette = palette,
        )
        if (transfer.status == TransferStatus.Ongoing) {
            Button(onClick = { onAbortReceiver(transfer.receiverId) }, modifier = Modifier.fillMaxWidth()) {
                Text("Stop receiver")
            }
        }
    }
}

@Composable
private fun FilePickerDialog(
    themePreference: ThemePreference,
    directoryOnly: Boolean,
    pickDirectoriesImmediately: Boolean,
    onDismiss: () -> Unit,
    onPick: (List<SendEntry>) -> Unit,
    onError: (String) -> Unit,
) {
    val palette = palette(themePreference)
    var currentDir by remember { mutableStateOf(File(SHARED_STORAGE_ROOT)) }
    var selected by remember { mutableStateOf<Set<SendEntry>>(emptySet()) }
    var pickerEntries by remember { mutableStateOf<List<PickerEntry>>(emptyList()) }
    var isListing by remember { mutableStateOf(true) }
    var listingError by remember { mutableStateOf<String?>(null) }
    var infoEntry by remember { mutableStateOf<PickerEntry?>(null) }
    val quickFolders = remember { commonPickerFolders() }
    val mainHandler = remember { Handler(Looper.getMainLooper()) }
    DisposableEffect(currentDir.absolutePath) {
        var cancelled = false
        isListing = true
        listingError = null
        pickerEntries = emptyList()
        val dir = currentDir
        val thread = Thread {
            val result = runCatching {
                val files = dir.listFiles() ?: throw IllegalStateException("Cannot read ${dir.absolutePath}")
                files
                    .asSequence()
                    .filter { !it.isHidden }
                    .map {
                        PickerEntry(
                            path = it.absolutePath,
                            name = it.name,
                            size = if (it.isDirectory) null else it.length(),
                            isDirectory = it.isDirectory,
                            lastModified = it.lastModified(),
                        )
                    }
                    .sortedWith(compareBy<PickerEntry> { !it.isDirectory }.thenBy { it.name.lowercase(Locale.US) })
                    .toList()
            }
            mainHandler.post {
                if (!cancelled) {
                    result
                        .onSuccess { pickerEntries = it }
                        .onFailure {
                            listingError = it.message ?: "Cannot read ${dir.absolutePath}"
                            onError(listingError ?: "Cannot read folder")
                        }
                    isListing = false
                }
            }
        }
        thread.start()
        onDispose {
            cancelled = true
            thread.interrupt()
        }
    }
    ThemedModal(
        palette = palette,
        onDismissRequest = onDismiss,
        title = if (directoryOnly) "Select folder" else "Select files or folders",
        body = {
            Column(verticalArrangement = Arrangement.spacedBy(8.dp)) {
                QuickFolderTabs(
                    folders = quickFolders,
                    currentPath = currentDir.absolutePath,
                    palette = palette,
                    onOpen = { path -> currentDir = File(path) },
                )
                Text(currentDir.absolutePath, fontSize = 11.sp, color = palette.textSoft, maxLines = 2)
                LazyColumn(
                    modifier = Modifier
                        .fillMaxWidth()
                        .heightIn(max = 520.dp),
                    verticalArrangement = Arrangement.spacedBy(8.dp),
                ) {
                    if (currentDir.absolutePath != SHARED_STORAGE_ROOT) {
                        item(key = "parent") {
                            Row(
                                modifier = Modifier
                                    .fillMaxWidth()
                                    .clip(RoundedCornerShape(12.dp))
                                    .background(palette.surface)
                                    .border(1.dp, palette.border, RoundedCornerShape(12.dp))
                                    .clickable { currentDir = currentDir.parentFile ?: File(SHARED_STORAGE_ROOT) }
                                    .padding(10.dp),
                                verticalAlignment = Alignment.CenterVertically,
                                horizontalArrangement = Arrangement.spacedBy(10.dp),
                            ) {
                                Icon(Icons.Rounded.Folder, contentDescription = null, tint = palette.text, modifier = Modifier.size(22.dp))
                                Column(modifier = Modifier.weight(1f)) {
                                    Text("..", color = palette.text, fontSize = 13.sp, fontWeight = FontWeight.SemiBold, maxLines = 1)
                                    Text("Parent folder", color = palette.textSoft, fontSize = 11.sp, maxLines = 1)
                                }
                            }
                        }
                    }
                    if (directoryOnly) {
                        item(key = "current") {
                            Box(
                                modifier = Modifier
                                    .fillMaxWidth()
                                    .clip(RoundedCornerShape(12.dp))
                                    .background(palette.surfaceStrong)
                                    .border(1.dp, palette.border, RoundedCornerShape(12.dp))
                                    .clickable {
                                        val entry = SendEntry(currentDir.absolutePath, null, true)
                                        if (pickDirectoriesImmediately) {
                                            onPick(listOf(entry))
                                        } else {
                                            selected = if (selected.contains(entry)) selected - entry else selected + entry
                                        }
                                    }
                                    .padding(horizontal = 12.dp, vertical = 10.dp),
                                contentAlignment = Alignment.Center,
                            ) {
                                Text(
                                    text = if (selected.any { it.path == currentDir.absolutePath && it.isDirectory }) {
                                        "Current folder selected"
                                    } else {
                                        "Use this folder"
                                    },
                                    color = palette.text,
                                    fontSize = 13.sp,
                                    fontWeight = FontWeight.SemiBold,
                                )
                            }
                        }
                    }
                    if (isListing) {
                        item(key = "loading") {
                            PickerLoadingRow(palette)
                        }
                    } else if (listingError != null) {
                        item(key = "error") {
                            Text(
                                text = listingError ?: "Cannot read folder",
                                color = Color(0xFFFF4D4F),
                                fontSize = 12.sp,
                                modifier = Modifier
                                    .fillMaxWidth()
                                    .clip(RoundedCornerShape(12.dp))
                                    .background(palette.surface)
                                    .border(1.dp, palette.border, RoundedCornerShape(12.dp))
                                    .padding(12.dp),
                            )
                        }
                    } else if (pickerEntries.isEmpty()) {
                        item(key = "empty") {
                            Text(
                                text = "This folder is empty",
                                color = palette.textSoft,
                                fontSize = 12.sp,
                                textAlign = TextAlign.Center,
                                modifier = Modifier
                                    .fillMaxWidth()
                                    .clip(RoundedCornerShape(12.dp))
                                    .background(palette.surface)
                                    .border(1.dp, palette.border, RoundedCornerShape(12.dp))
                                    .padding(12.dp),
                            )
                        }
                    }
                    items(
                        if (directoryOnly) pickerEntries.filter { it.isDirectory } else pickerEntries,
                        key = { it.path },
                    ) { pickerEntry ->
                        val entry = SendEntry(pickerEntry.path, pickerEntry.size, pickerEntry.isDirectory)
                        FilePickerRow(
                            path = pickerEntry.path,
                            name = pickerEntry.name,
                            detail = if (pickerEntry.isDirectory) "Folder" else formatSize(pickerEntry.size ?: 0L),
                            icon = if (pickerEntry.isDirectory) Icons.Rounded.Folder else Icons.Rounded.InsertDriveFile,
                            palette = palette,
                            selected = selected.contains(entry),
                            canOpen = pickerEntry.isDirectory,
                            onOpen = {
                                if (pickerEntry.isDirectory) {
                                    currentDir = File(pickerEntry.path)
                                }
                            },
                            onSelect = {
                                if (directoryOnly && pickDirectoriesImmediately && entry.isDirectory) {
                                    onPick(listOf(entry))
                                } else {
                                    selected = if (selected.contains(entry)) selected - entry else selected + entry
                                }
                            },
                            onInfo = { infoEntry = pickerEntry },
                        )
                    }
                }
            }
        },
        actions = {
            TextButton(onClick = onDismiss) {
                Text("Cancel")
            }
            if (!pickDirectoriesImmediately) {
                Button(onClick = { onPick(selected.toList()) }, enabled = selected.isNotEmpty()) {
                    Text("Add selected")
                }
            }
        },
    )
    infoEntry?.let { entry ->
        ThemedModal(
            palette = palette,
            onDismissRequest = { infoEntry = null },
            title = "Item info",
            body = {
                FileInfoDetails(entry = entry, palette = palette)
            },
            actions = {
                Button(onClick = { infoEntry = null }) {
                    Text("Close")
                }
            },
        )
    }
}

@Composable
private fun QuickFolderTabs(
    folders: List<QuickFolder>,
    currentPath: String,
    palette: HomePalette,
    onOpen: (String) -> Unit,
) {
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .horizontalScroll(rememberScrollState()),
        horizontalArrangement = Arrangement.spacedBy(8.dp),
        verticalAlignment = Alignment.CenterVertically,
    ) {
        folders.forEach { folder ->
            val selected = currentPath == folder.path
            Row(
                modifier = Modifier
                    .clip(RoundedCornerShape(999.dp))
                    .background(if (selected) Color(0xFF1976FF) else palette.surface)
                    .border(1.dp, if (selected) Color(0xFF1976FF) else palette.border, RoundedCornerShape(999.dp))
                    .clickable { onOpen(folder.path) }
                    .padding(horizontal = 12.dp, vertical = 8.dp),
                verticalAlignment = Alignment.CenterVertically,
                horizontalArrangement = Arrangement.spacedBy(6.dp),
            ) {
                Icon(
                    imageVector = folder.icon,
                    contentDescription = null,
                    tint = if (selected) Color.White else palette.text,
                    modifier = Modifier.size(15.dp),
                )
                Text(
                    text = folder.label,
                    color = if (selected) Color.White else palette.text,
                    fontSize = 12.sp,
                    fontWeight = FontWeight.Bold,
                    maxLines = 1,
                )
            }
        }
    }
}

@Composable
private fun PickerLoadingRow(palette: HomePalette) {
    val transition = rememberInfiniteTransition(label = "picker-loading")
    val rotation = transition.animateFloat(
        initialValue = 0f,
        targetValue = 360f,
        animationSpec = infiniteRepeatable(
            animation = tween(durationMillis = 1000, easing = LinearEasing),
            repeatMode = RepeatMode.Restart,
        ),
        label = "picker-loading-rotation",
    ).value
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .clip(RoundedCornerShape(12.dp))
            .background(palette.surface)
            .border(1.dp, palette.border, RoundedCornerShape(12.dp))
            .padding(12.dp),
        verticalAlignment = Alignment.CenterVertically,
        horizontalArrangement = Arrangement.spacedBy(10.dp),
    ) {
        Box(
            modifier = Modifier
                .size(34.dp)
                .clip(RoundedCornerShape(10.dp))
                .background(Color(0x221976FF)),
            contentAlignment = Alignment.Center,
        ) {
            Icon(
                imageVector = Icons.Rounded.Folder,
                contentDescription = null,
                tint = Color(0xFF1976FF),
                modifier = Modifier
                    .size(19.dp)
                    .rotate(rotation),
            )
        }
        Column {
            Text("Loading folder", color = palette.text, fontSize = 13.sp, fontWeight = FontWeight.SemiBold)
            Text("Reading files and folders", color = palette.textSoft, fontSize = 11.sp)
        }
    }
}

@Composable
private fun FilePickerRow(
    path: String,
    name: String,
    detail: String,
    icon: ImageVector,
    palette: HomePalette,
    selected: Boolean,
    canOpen: Boolean,
    onOpen: () -> Unit,
    onSelect: () -> Unit,
    onInfo: () -> Unit,
) {
    val rowClick = if (canOpen) Modifier.clickable(onClick = onOpen) else Modifier
    val imageBitmap = remember(path) {
        decodeMediaPreview(path)
    }
    Column(
        modifier = Modifier
            .fillMaxWidth()
            .clip(RoundedCornerShape(12.dp))
            .background(if (selected) Color(0x331976FF) else palette.surface)
            .border(1.dp, if (selected) Color(0xFF1976FF) else palette.border, RoundedCornerShape(12.dp))
            .then(rowClick)
            .padding(10.dp),
        verticalArrangement = Arrangement.spacedBy(9.dp),
    ) {
        if (imageBitmap != null) {
            Image(
                bitmap = imageBitmap.asImageBitmap(),
                contentDescription = null,
                contentScale = ContentScale.Crop,
                modifier = Modifier
                    .fillMaxWidth()
                    .aspectRatio(1f)
                    .clip(RoundedCornerShape(10.dp))
                    .background(palette.surfaceStrong),
            )
        }
        Row(
            modifier = Modifier.fillMaxWidth(),
            verticalAlignment = Alignment.CenterVertically,
            horizontalArrangement = Arrangement.spacedBy(10.dp),
        ) {
            Icon(icon, contentDescription = null, tint = palette.text, modifier = Modifier.size(22.dp))
            Column(
                modifier = Modifier.weight(1f),
            ) {
                Text(name, color = palette.text, fontSize = 13.sp, fontWeight = FontWeight.SemiBold, maxLines = 1)
                Text(detail, color = palette.textSoft, fontSize = 11.sp, maxLines = 1)
            }
            Row(horizontalArrangement = Arrangement.spacedBy(6.dp), verticalAlignment = Alignment.CenterVertically) {
                Box(
                    modifier = Modifier
                        .size(32.dp)
                        .clip(RoundedCornerShape(999.dp))
                        .background(palette.surfaceStrong)
                        .border(1.dp, palette.border, RoundedCornerShape(999.dp))
                        .clickable(onClick = onInfo),
                    contentAlignment = Alignment.Center,
                ) {
                    Icon(Icons.Rounded.Info, contentDescription = "Info", tint = palette.text, modifier = Modifier.size(16.dp))
                }
                Box(
                    modifier = Modifier
                        .clip(RoundedCornerShape(999.dp))
                        .background(if (selected) Color(0xFF1976FF) else palette.surfaceStrong)
                        .border(1.dp, if (selected) Color(0xFF1976FF) else palette.border, RoundedCornerShape(999.dp))
                        .clickable(onClick = onSelect)
                        .padding(horizontal = 10.dp, vertical = 6.dp),
                    contentAlignment = Alignment.Center,
                ) {
                    Text(if (selected) "Selected" else "Select", color = if (selected) Color.White else palette.text, fontSize = 11.sp)
                }
            }
        }
    }
}

@Composable
private fun FileInfoDetails(entry: PickerEntry, palette: HomePalette) {
    Column(verticalArrangement = Arrangement.spacedBy(8.dp)) {
        FileInfoRow("Name", entry.name, palette)
        FileInfoRow("Type", if (entry.isDirectory) "Folder" else "File", palette)
        FileInfoRow("Size", if (entry.isDirectory) "Folder" else formatSize(entry.size ?: 0L), palette)
        FileInfoRow("Modified", formatModifiedTime(entry.lastModified), palette)
        FileInfoRow("Path", entry.path, palette)
    }
}

@Composable
private fun FileInfoRow(label: String, value: String, palette: HomePalette) {
    Column(
        modifier = Modifier
            .fillMaxWidth()
            .clip(RoundedCornerShape(12.dp))
            .background(palette.surface)
            .border(1.dp, palette.border, RoundedCornerShape(12.dp))
            .padding(10.dp),
        verticalArrangement = Arrangement.spacedBy(3.dp),
    ) {
        Text(label, color = palette.textSoft, fontSize = 11.sp, fontWeight = FontWeight.SemiBold)
        Text(value, color = palette.text, fontSize = 13.sp, lineHeight = 17.sp)
    }
}

private fun decodeMediaPreview(path: String) = when {
    isPreviewableImage(path) -> decodeImagePreview(path)
    isPreviewableVideo(path) -> decodeVideoPreview(path)
    else -> null
}

private fun isPreviewableImage(path: String): Boolean {
    val lower = path.lowercase(Locale.US)
    return lower.endsWith(".jpg") ||
        lower.endsWith(".jpeg") ||
        lower.endsWith(".png") ||
        lower.endsWith(".webp") ||
        lower.endsWith(".gif") ||
        lower.endsWith(".bmp")
}

private fun isPreviewableVideo(path: String): Boolean {
    val lower = path.lowercase(Locale.US)
    return lower.endsWith(".mp4") ||
        lower.endsWith(".m4v") ||
        lower.endsWith(".mov") ||
        lower.endsWith(".mkv") ||
        lower.endsWith(".webm") ||
        lower.endsWith(".3gp") ||
        lower.endsWith(".3gpp") ||
        lower.endsWith(".avi")
}

private fun decodeImagePreview(path: String) = runCatching {
    val bounds = BitmapFactory.Options().apply {
        inJustDecodeBounds = true
    }
    BitmapFactory.decodeFile(path, bounds)
    if (bounds.outWidth <= 0 || bounds.outHeight <= 0) {
        return@runCatching null
    }
    var sampleSize = 1
    val maxSide = bounds.outWidth.coerceAtLeast(bounds.outHeight)
    while (maxSide / sampleSize > 512) {
        sampleSize *= 2
    }
    val options = BitmapFactory.Options().apply {
        inSampleSize = sampleSize
    }
    BitmapFactory.decodeFile(path, options)
}.getOrNull()

private fun decodeVideoPreview(path: String) = runCatching {
    val retriever = MediaMetadataRetriever()
    try {
        retriever.setDataSource(path)
        retriever.getFrameAtTime(1_000_000, MediaMetadataRetriever.OPTION_CLOSEST_SYNC)
    } finally {
        retriever.release()
    }
}.getOrNull()

@Composable
private fun ReceiveScreen(
    themePreference: ThemePreference,
    joinCode: String,
    saveDirectory: String,
    overwrite: Boolean,
    flowStage: ReceiveFlowStage,
    manifestTotalSize: Long,
    manifestSummaryFilesCount: Int,
    manifestSummaryTotalSize: Long,
    transferProgress: ReceiveTransferProgress,
    savedDevices: List<SavedDevice>,
    canReceive: Boolean,
    onBack: () -> Unit,
    onJoinCodeChange: (String) -> Unit,
    onSelectSavedDevice: (SavedDevice) -> Unit,
    onSaveCurrentDevice: () -> Unit,
    onRemoveSavedDevice: (String) -> Unit,
    onSelectDirectory: () -> Unit,
    onOverwriteChange: (Boolean) -> Unit,
    onReceive: () -> Unit,
    onAbort: () -> Unit,
    onCopySavePath: () -> Unit,
    onRetry: () -> Unit,
) {
    val palette = palette(themePreference)
    Box(
        modifier = Modifier
            .fillMaxSize()
            .background(Brush.linearGradient(listOf(palette.bg, palette.bgAccent)))
            .safeDrawingPadding()
            .padding(horizontal = 12.dp, vertical = 10.dp),
        contentAlignment = Alignment.TopCenter,
    ) {
        Box(
            modifier = Modifier
                .widthIn(max = 760.dp)
                .fillMaxSize(),
        ) {
            if (flowStage == ReceiveFlowStage.Idle) {
                ReceiveIdleContent(
                    palette = palette,
                    joinCode = joinCode,
                    saveDirectory = saveDirectory,
                    overwrite = overwrite,
                    savedDevices = savedDevices,
                    canReceive = canReceive,
                    onBack = onBack,
                    onJoinCodeChange = onJoinCodeChange,
                    onSelectSavedDevice = onSelectSavedDevice,
                    onSaveCurrentDevice = onSaveCurrentDevice,
                    onRemoveSavedDevice = onRemoveSavedDevice,
                    onSelectDirectory = onSelectDirectory,
                    onOverwriteChange = onOverwriteChange,
                    onReceive = onReceive,
                )
            } else {
                ReceiveStateContent(
                    palette = palette,
                    flowStage = flowStage,
                    saveDirectory = saveDirectory,
                    manifestTotalSize = manifestTotalSize,
                    manifestSummaryFilesCount = manifestSummaryFilesCount,
                    manifestSummaryTotalSize = manifestSummaryTotalSize,
                transferProgress = transferProgress,
                onBack = onBack,
                onAbort = onAbort,
                onCopySavePath = onCopySavePath,
                onRetry = onRetry,
            )
        }
        }
    }
}

@Composable
private fun ReceiveIdleContent(
    palette: HomePalette,
    joinCode: String,
    saveDirectory: String,
    overwrite: Boolean,
    savedDevices: List<SavedDevice>,
    canReceive: Boolean,
    onBack: () -> Unit,
    onJoinCodeChange: (String) -> Unit,
    onSelectSavedDevice: (SavedDevice) -> Unit,
    onSaveCurrentDevice: () -> Unit,
    onRemoveSavedDevice: (String) -> Unit,
    onSelectDirectory: () -> Unit,
    onOverwriteChange: (Boolean) -> Unit,
    onReceive: () -> Unit,
) {
    Column(
        modifier = Modifier.fillMaxSize(),
        verticalArrangement = Arrangement.spacedBy(12.dp),
    ) {
        SenderTopActions(palette = palette, onBack = onBack, onAbort = null)
        Column(
            modifier = Modifier
                .fillMaxWidth()
                .weight(1f)
                .clip(RoundedCornerShape(22.dp))
                .background(palette.surfaceStrong)
                .border(1.dp, palette.border, RoundedCornerShape(22.dp))
                .padding(16.dp),
            horizontalAlignment = Alignment.CenterHorizontally,
            verticalArrangement = Arrangement.spacedBy(12.dp),
        ) {
            Box(
                modifier = Modifier
                    .size(66.dp)
                    .clip(RoundedCornerShape(20.dp))
                    .background(Color(0x2200A989)),
                contentAlignment = Alignment.Center,
            ) {
                Icon(Icons.Rounded.Inbox, contentDescription = null, tint = Color(0xFF00A989), modifier = Modifier.size(34.dp))
            }
            Text("Join a transfer", color = palette.text, fontSize = 24.sp, fontWeight = FontWeight.Bold, textAlign = TextAlign.Center)
            Text("Enter the code shared by the sender to begin receiving files", color = palette.textSoft, fontSize = 14.sp, lineHeight = 19.sp, textAlign = TextAlign.Center)
            OutlinedTextField(
                value = joinCode,
                onValueChange = onJoinCodeChange,
                label = { Text("Join code") },
                placeholder = { Text("THRU-JOIN-CODE-HERE") },
                singleLine = true,
                modifier = Modifier.fillMaxWidth(),
            )
            Button(onClick = onSaveCurrentDevice, modifier = Modifier.fillMaxWidth()) {
                Text("Save device")
            }
            Column(
                modifier = Modifier.fillMaxWidth(),
                verticalArrangement = Arrangement.spacedBy(6.dp),
            ) {
                Text("Save location", color = palette.text, fontSize = 13.sp, fontWeight = FontWeight.SemiBold)
                Row(
                    modifier = Modifier.fillMaxWidth(),
                    horizontalArrangement = Arrangement.spacedBy(8.dp),
                    verticalAlignment = Alignment.CenterVertically,
                ) {
                    Text(
                        saveDirectory,
                        color = palette.textSoft,
                        fontSize = 11.sp,
                        maxLines = 2,
                        modifier = Modifier
                            .weight(1f)
                            .clip(RoundedCornerShape(12.dp))
                            .background(palette.surface)
                            .border(1.dp, palette.border, RoundedCornerShape(12.dp))
                            .padding(10.dp),
                    )
                    Button(onClick = onSelectDirectory) {
                        Text("Select folder")
                    }
                }
            }
            Column(
                modifier = Modifier
                    .fillMaxWidth()
                    .weight(1f),
                verticalArrangement = Arrangement.spacedBy(8.dp),
            ) {
                Text("Saved devices", color = palette.text, fontSize = 16.sp, fontWeight = FontWeight.Bold)
                if (savedDevices.isEmpty()) {
                    Text("Saved devices appear here", color = palette.textSoft, fontSize = 12.sp)
                } else {
                    LazyColumn(
                        modifier = Modifier.weight(1f),
                        verticalArrangement = Arrangement.spacedBy(8.dp),
                    ) {
                        items(savedDevices, key = { it.id }) { device ->
                            SavedDeviceRow(device, palette, onSelectSavedDevice, onRemoveSavedDevice)
                        }
                    }
                }
            }
            SettingsSwitchRow(
                label = "Overwrite existing files",
                hint = "Turn off automatic resume and overwrite any existing files",
                checked = overwrite,
                palette = palette,
                onCheckedChange = onOverwriteChange,
            )
        }
        Button(onClick = onReceive, enabled = canReceive, modifier = Modifier.fillMaxWidth()) {
            Text("Receive")
        }
    }
}

@Composable
private fun SavedDeviceRow(
    device: SavedDevice,
    palette: HomePalette,
    onSelect: (SavedDevice) -> Unit,
    onRemove: (String) -> Unit,
) {
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .clip(RoundedCornerShape(12.dp))
            .background(palette.surface)
            .border(1.dp, palette.border, RoundedCornerShape(12.dp))
            .padding(10.dp),
        horizontalArrangement = Arrangement.spacedBy(10.dp),
        verticalAlignment = Alignment.CenterVertically,
    ) {
        Column(
            modifier = Modifier
                .weight(1f)
                .clickable { onSelect(device) },
        ) {
            Text(device.name, color = palette.text, fontSize = 13.sp, fontWeight = FontWeight.SemiBold)
            Text(device.joinCode, color = palette.textSoft, fontSize = 11.sp)
        }
        IconButtonLike(Icons.Rounded.Delete, palette) { onRemove(device.id) }
    }
}

@Composable
private fun ReceiveStateContent(
    palette: HomePalette,
    flowStage: ReceiveFlowStage,
    saveDirectory: String,
    manifestTotalSize: Long,
    manifestSummaryFilesCount: Int,
    manifestSummaryTotalSize: Long,
    transferProgress: ReceiveTransferProgress,
    onBack: () -> Unit,
    onAbort: () -> Unit,
    onCopySavePath: () -> Unit,
    onRetry: () -> Unit,
) {
    Column(
        modifier = Modifier.fillMaxSize(),
        verticalArrangement = Arrangement.spacedBy(12.dp),
    ) {
        SenderTopActions(palette = palette, onBack = onBack, onAbort = if (flowStage == ReceiveFlowStage.Transfer) onAbort else null)
        Column(
            modifier = Modifier
                .fillMaxWidth()
                .weight(1f)
                .clip(RoundedCornerShape(22.dp))
                .background(palette.surfaceStrong)
                .border(1.dp, palette.border, RoundedCornerShape(22.dp))
                .padding(16.dp),
            horizontalAlignment = Alignment.CenterHorizontally,
            verticalArrangement = Arrangement.Center,
        ) {
            when (flowStage) {
                ReceiveFlowStage.Starting -> StatePanel(Icons.Rounded.Settings, "Getting ready", "Preparing to receive your files", palette, loading = true)
                ReceiveFlowStage.Connecting -> StatePanel(Icons.Rounded.Download, "Connecting", "Trying to connect to your sender", palette, loading = true)
                ReceiveFlowStage.Connected -> StatePanel(Icons.Rounded.Download, "Connected", "Connection is ready", palette)
                ReceiveFlowStage.JoiningSession -> StatePanel(Icons.Rounded.Download, "Joining", "Checking your code and joining the transfer", palette, loading = true)
                ReceiveFlowStage.P2pStart -> StatePanel(Icons.Rounded.Upload, "Building direct link", "Setting up a fast path between devices", palette, loading = true)
                ReceiveFlowStage.P2pSuccess -> StatePanel(Icons.Rounded.Upload, "Direct link ready", "Your transfer path is established", palette)
                ReceiveFlowStage.ManifestReceiving -> StatePanel(Icons.Rounded.InsertDriveFile, "Reading file list", "Getting the file plan from the sender", palette, loading = true) {
                    SummaryPill("Manifest size: ${formatSize(manifestTotalSize)}", Icons.Rounded.InsertDriveFile, palette, Modifier.fillMaxWidth())
                }
                ReceiveFlowStage.ManifestParsing -> StatePanel(Icons.Rounded.InsertDriveFile, "Preparing your transfer", "Checking file details and finding the best place to continue", palette, loading = true)
                ReceiveFlowStage.QuicReady -> StatePanel(Icons.Rounded.Upload, "Secure channel ready", "Preparing to move files", palette)
                ReceiveFlowStage.Transfer, ReceiveFlowStage.Complete, ReceiveFlowStage.Failed -> ReceiveTransferPanel(flowStage, saveDirectory, manifestSummaryFilesCount, manifestSummaryTotalSize, transferProgress, palette, onAbort, onCopySavePath, onRetry)
                ReceiveFlowStage.Idle -> Unit
            }
        }
    }
}

@Composable
private fun ReceiveTransferPanel(
    flowStage: ReceiveFlowStage,
    saveDirectory: String,
    filesCount: Int,
    totalSize: Long,
    progress: ReceiveTransferProgress,
    palette: HomePalette,
    onAbort: () -> Unit,
    onCopySavePath: () -> Unit,
    onRetry: () -> Unit,
) {
    Column(
        modifier = Modifier
            .fillMaxWidth()
            .verticalScroll(rememberScrollState()),
        horizontalAlignment = Alignment.CenterHorizontally,
        verticalArrangement = Arrangement.spacedBy(14.dp),
    ) {
        val title = when (flowStage) {
            ReceiveFlowStage.Complete -> "Transfer complete"
            ReceiveFlowStage.Failed -> "Transfer stopped early"
            else -> "Receiving files"
        }
        val body = when (flowStage) {
            ReceiveFlowStage.Complete -> "All files were received successfully"
            ReceiveFlowStage.Failed -> "The transfer ran into a problem and could not continue"
            else -> "Files are moving now"
        }
        StatePanel(Icons.Rounded.Download, title, body, palette, loading = flowStage == ReceiveFlowStage.Transfer)
        SummaryPillRow(
            firstLabel = "Expected files: $filesCount",
            firstIcon = Icons.Rounded.InsertDriveFile,
            secondLabel = "Expected size: ${formatSize(totalSize)}",
            secondIcon = Icons.Rounded.Upload,
            palette = palette,
        )
        Text(saveDirectory, color = palette.textSoft, fontSize = 11.sp, maxLines = 2, textAlign = TextAlign.Center)
        Text("${progress.percent.roundToInt().coerceIn(0, 100)}%", color = palette.text, fontSize = 32.sp, fontWeight = FontWeight.Bold)
        SummaryPillRow(
            firstLabel = "Speed: ${formatThroughput(progress.ewmaThroughput)}",
            firstIcon = Icons.Rounded.Download,
            secondLabel = "Moved: ${formatSize(progress.bytesMoved)}",
            secondIcon = Icons.Rounded.Download,
            palette = palette,
        )
        SummaryPillRow(
            firstLabel = "Skipped: ${formatSize(progress.skippedBytes)}",
            firstIcon = Icons.Rounded.InsertDriveFile,
            secondLabel = "Files: ${progress.filesMoved}/${progress.totalExpectedFilesCount}",
            secondIcon = Icons.Rounded.Folder,
            palette = palette,
        )
        SummaryPillRow(
            firstLabel = "Route: ${if (progress.isRelayed) "Relayed" else "Direct"}",
            firstIcon = Icons.Rounded.Upload,
            secondLabel = "ETA: ${formatEta(totalSize, progress.bytesMoved, progress.skippedBytes, progress.ewmaThroughput)}",
            secondIcon = Icons.Rounded.Settings,
            palette = palette,
        )
        when (flowStage) {
            ReceiveFlowStage.Transfer -> Button(onClick = onAbort, modifier = Modifier.fillMaxWidth()) { Text("Stop transfer") }
            ReceiveFlowStage.Complete -> Button(onClick = onCopySavePath, modifier = Modifier.fillMaxWidth()) { Text("Copy saved path") }
            ReceiveFlowStage.Failed -> Button(onClick = onRetry, modifier = Modifier.fillMaxWidth()) { Text("Retry") }
            else -> Unit
        }
    }
}

@Composable
private fun SettingsShell(
    themePreference: ThemePreference,
    settings: AppSettings,
    onPatchSettings: ((AppSettings) -> AppSettings) -> Unit,
    onRestoreDefaults: () -> Unit,
    onToggleTheme: () -> Unit,
    onOpenPrivacyPolicy: () -> Unit,
    onBack: () -> Unit,
) {
    val palette = palette(themePreference)
    val errors = validateSettings(settings)
    Box(
        modifier = Modifier
            .fillMaxSize()
            .background(Brush.linearGradient(listOf(palette.bg, palette.bgAccent)))
            .safeDrawingPadding()
            .padding(horizontal = 12.dp, vertical = 10.dp),
        contentAlignment = Alignment.TopCenter,
    ) {
        Column(
            modifier = Modifier
                .widthIn(max = 760.dp)
                .fillMaxSize()
                .verticalScroll(rememberScrollState()),
            verticalArrangement = Arrangement.spacedBy(12.dp),
        ) {
            Row(
                modifier = Modifier.fillMaxWidth(),
                verticalAlignment = Alignment.CenterVertically,
                horizontalArrangement = Arrangement.spacedBy(10.dp),
            ) {
                Box(
                    modifier = Modifier
                        .clip(RoundedCornerShape(12.dp))
                        .background(palette.surfaceStrong)
                        .border(1.dp, palette.border, RoundedCornerShape(12.dp))
                        .clickable(onClick = onBack)
                        .padding(horizontal = 12.dp, vertical = 9.dp),
                    contentAlignment = Alignment.Center,
                ) {
                    Icon(
                        imageVector = Icons.AutoMirrored.Rounded.ArrowBack,
                        contentDescription = "Back",
                        tint = palette.text,
                        modifier = Modifier.size(20.dp),
                    )
                }
                Text(
                    text = "Settings",
                    color = palette.text,
                    fontSize = 24.sp,
                    fontWeight = FontWeight.Bold,
                    modifier = Modifier.weight(1f),
                )
                Box(
                    modifier = Modifier
                        .clip(RoundedCornerShape(12.dp))
                        .background(palette.surfaceStrong)
                        .border(1.dp, palette.border, RoundedCornerShape(12.dp))
                        .clickable(onClick = onRestoreDefaults)
                        .padding(horizontal = 12.dp, vertical = 9.dp),
                    contentAlignment = Alignment.Center,
                ) {
                    Text(
                        text = "Restore defaults",
                        color = palette.text,
                        fontSize = 12.sp,
                        fontWeight = FontWeight.SemiBold,
                    )
                }
            }
            SettingsNotice(palette)
            SettingsActionButton(
                label = "View privacy policy",
                hint = "Opens the Thruflux privacy policy in your browser",
                icon = Icons.Rounded.Lock,
                palette = palette,
                onClick = onOpenPrivacyPolicy,
            )
            SettingsSectionTitle("Connection", palette)
            SettingsTextField(
                label = "Server URL",
                value = settings.serverUrl,
                hint = "Address used when creating or joining a transfer session",
                error = errors.serverUrl,
                palette = palette,
                onValueChange = { value -> onPatchSettings { it.copy(serverUrl = value) } },
            )
            SettingsTextField(
                label = "STUN server",
                value = settings.stunServer,
                hint = "Used to help establish direct connectivity",
                error = errors.stunServer,
                palette = palette,
                onValueChange = { value -> onPatchSettings { it.copy(stunServer = value) } },
            )
            SettingsTextField(
                label = "TURN servers",
                value = settings.turnServers,
                hint = "Enter one per line",
                error = errors.turnServers,
                palette = palette,
                singleLine = false,
                onValueChange = { value -> onPatchSettings { it.copy(turnServers = value) } },
            )
            SettingsSwitchRow(
                label = "Force TURN",
                hint = "Force routed mode instead of direct connectivity",
                checked = settings.forceTurn,
                palette = palette,
                onCheckedChange = { value -> onPatchSettings { it.copy(forceTurn = value) } },
            )
            SettingsSectionTitle("Transport", palette)
            ByteSliderRow(
                label = "QUIC connection window bytes",
                hint = "Maximum buffer budget for the full connection",
                value = settings.quicConnWindowBytes,
                min = QCW_MIN,
                max = QCW_MAX,
                step = 1024L * 1024L,
                palette = palette,
                onValueChange = { value ->
                    onPatchSettings {
                        it.copy(
                            quicConnWindowBytes = value,
                            quicStreamWindowBytes = it.quicStreamWindowBytes.coerceAtMost(value),
                        )
                    }
                },
            )
            ByteSliderRow(
                label = "QUIC stream window bytes",
                hint = "Maximum buffer budget for each stream",
                value = settings.quicStreamWindowBytes,
                min = QSW_MIN,
                max = QSW_MAX,
                step = 256L * 1024L,
                error = errors.quicRelation,
                palette = palette,
                onValueChange = { value ->
                    onPatchSettings {
                        it.copy(quicStreamWindowBytes = value.coerceAtMost(it.quicConnWindowBytes))
                    }
                },
            )
            ByteSliderRow(
                label = "UDP buffer bytes",
                hint = "System UDP buffer target for transport throughput",
                value = settings.udpBufferBytes,
                min = UDP_MIN,
                max = UDP_MAX,
                step = 1024L * 1024L,
                palette = palette,
                onValueChange = { value -> onPatchSettings { it.copy(udpBufferBytes = value) } },
            )
            IntSliderRow(
                label = "Max number of receivers",
                hint = "How many receivers can join at once",
                value = settings.maxReceivers,
                min = MAX_RECEIVERS_MIN,
                max = MAX_RECEIVERS_MAX,
                palette = palette,
                onValueChange = { value -> onPatchSettings { it.copy(maxReceivers = value) } },
            )
            SettingsSectionTitle("Transfer", palette)
            SettingsSwitchRow(
                label = "Random join codes",
                hint = "Use a new code for every send",
                checked = settings.randomJoinCodeMode,
                palette = palette,
                onCheckedChange = { value -> onPatchSettings { it.copy(randomJoinCodeMode = value) } },
            )
            SettingsSwitchRow(
                label = "Overwrite existing files",
                hint = "When disabled, existing files are kept. Disable overwrite to effectively allow auto resume",
                checked = settings.overwrite,
                palette = palette,
                onCheckedChange = { value -> onPatchSettings { it.copy(overwrite = value) } },
            )
            SettingsSectionTitle("Notifications", palette)
            SettingsSwitchRow(
                label = "Receiver transfer complete",
                hint = "Alert when your receive session finishes",
                checked = settings.notifyReceiverSessionComplete,
                palette = palette,
                onCheckedChange = { value -> onPatchSettings { it.copy(notifyReceiverSessionComplete = value) } },
            )
            SettingsSwitchRow(
                label = "New receiver joined",
                hint = "Alert when someone starts receiving from you",
                checked = settings.notifySenderReceiverJoined,
                palette = palette,
                onCheckedChange = { value -> onPatchSettings { it.copy(notifySenderReceiverJoined = value) } },
            )
            SettingsSwitchRow(
                label = "Receiver transfer complete (sender side)",
                hint = "Alert when a receiver finishes downloading",
                checked = settings.notifySenderReceiverComplete,
                palette = palette,
                onCheckedChange = { value -> onPatchSettings { it.copy(notifySenderReceiverComplete = value) } },
            )
            SettingsSwitchRow(
                label = "Transfer failed",
                hint = "Alert when a transfer fails on send or receive",
                checked = settings.notifyTransferFailure,
                palette = palette,
                onCheckedChange = { value -> onPatchSettings { it.copy(notifyTransferFailure = value) } },
            )
            SettingsSectionTitle("Appearance", palette)
            SettingsSwitchRow(
                label = "Dark theme",
                hint = "Light and dark are saved locally on this device",
                checked = themePreference == ThemePreference.Dark,
                palette = palette,
                onCheckedChange = { onToggleTheme() },
            )
        }
    }
}

@Composable
private fun SettingsNotice(palette: HomePalette) {
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .clip(RoundedCornerShape(14.dp))
            .background(Color(0xFFFFF4CC))
            .border(1.dp, Color(0xFFFFC857), RoundedCornerShape(14.dp))
            .padding(12.dp),
        horizontalArrangement = Arrangement.spacedBy(10.dp),
        verticalAlignment = Alignment.Top,
    ) {
        Box(
            modifier = Modifier
                .size(24.dp)
                .clip(CircleShape)
                .background(Color(0xFFFFC857)),
            contentAlignment = Alignment.Center,
        ) {
            Text(
                text = "!",
                color = Color(0xFF5C3B00),
                fontSize = 16.sp,
                fontWeight = FontWeight.Bold,
            )
        }
        Text(
            text = "Unless you know what you are doing, you are better off not modifying these settings.",
            color = Color(0xFF5C3B00),
            fontSize = 12.sp,
            lineHeight = 16.sp,
            fontWeight = FontWeight.SemiBold,
            modifier = Modifier.weight(1f),
        )
    }
}

@Composable
private fun SettingsSectionTitle(text: String, palette: HomePalette) {
    Text(
        text = text,
        color = palette.text,
        fontSize = 16.sp,
        fontWeight = FontWeight.Bold,
        modifier = Modifier.padding(top = 4.dp),
    )
}

@Composable
private fun SettingsActionButton(
    label: String,
    hint: String,
    icon: ImageVector,
    palette: HomePalette,
    onClick: () -> Unit,
) {
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .clip(RoundedCornerShape(14.dp))
            .background(palette.surfaceStrong)
            .border(1.dp, palette.border, RoundedCornerShape(14.dp))
            .clickable(onClick = onClick)
            .padding(12.dp),
        verticalAlignment = Alignment.CenterVertically,
        horizontalArrangement = Arrangement.spacedBy(10.dp),
    ) {
        Box(
            modifier = Modifier
                .size(34.dp)
                .clip(RoundedCornerShape(10.dp))
                .background(Color(0x221976FF)),
            contentAlignment = Alignment.Center,
        ) {
            Icon(icon, contentDescription = null, tint = Color(0xFF1976FF), modifier = Modifier.size(18.dp))
        }
        Column(modifier = Modifier.weight(1f), verticalArrangement = Arrangement.spacedBy(3.dp)) {
            Text(label, color = palette.text, fontSize = 14.sp, fontWeight = FontWeight.SemiBold)
            Text(hint, color = palette.textSoft, fontSize = 11.sp, lineHeight = 14.sp)
        }
    }
}

@Composable
private fun SettingsTextField(
    label: String,
    value: String,
    hint: String,
    error: String?,
    palette: HomePalette,
    singleLine: Boolean = true,
    onValueChange: (String) -> Unit,
) {
    Column(verticalArrangement = Arrangement.spacedBy(4.dp)) {
        OutlinedTextField(
            value = value,
            onValueChange = onValueChange,
            label = { Text(label) },
            singleLine = singleLine,
            minLines = if (singleLine) 1 else 3,
            maxLines = if (singleLine) 1 else 5,
            keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Uri),
            colors = OutlinedTextFieldDefaults.colors(
                focusedTextColor = palette.text,
                unfocusedTextColor = palette.text,
                focusedLabelColor = palette.text,
                unfocusedLabelColor = palette.textSoft,
                focusedBorderColor = palette.text,
                unfocusedBorderColor = palette.border,
                cursorColor = palette.text,
                focusedContainerColor = Color.Transparent,
                unfocusedContainerColor = Color.Transparent,
            ),
            modifier = Modifier.fillMaxWidth(),
        )
        Text(text = hint, color = palette.textSoft, fontSize = 11.sp, lineHeight = 14.sp)
        if (error != null) {
            Text(text = error, color = Color(0xFFFF4D4F), fontSize = 11.sp, lineHeight = 14.sp)
        }
    }
}

@Composable
private fun SettingsSwitchRow(
    label: String,
    hint: String,
    checked: Boolean,
    palette: HomePalette,
    onCheckedChange: (Boolean) -> Unit,
) {
    Column(
        modifier = Modifier
            .fillMaxWidth()
            .clip(RoundedCornerShape(14.dp))
            .background(palette.surfaceStrong)
            .border(1.dp, palette.border, RoundedCornerShape(14.dp))
            .padding(12.dp),
        verticalArrangement = Arrangement.spacedBy(6.dp),
    ) {
        Row(
            modifier = Modifier.fillMaxWidth(),
            verticalAlignment = Alignment.CenterVertically,
            horizontalArrangement = Arrangement.SpaceBetween,
        ) {
            Text(
                text = label,
                color = palette.text,
                fontSize = 14.sp,
                fontWeight = FontWeight.SemiBold,
                modifier = Modifier.weight(1f),
            )
            Switch(checked = checked, onCheckedChange = onCheckedChange)
        }
        Text(text = hint, color = palette.textSoft, fontSize = 11.sp, lineHeight = 14.sp)
    }
}

@Composable
private fun ByteSliderRow(
    label: String,
    hint: String,
    value: Long,
    min: Long,
    max: Long,
    step: Long,
    palette: HomePalette,
    error: String? = null,
    onValueChange: (Long) -> Unit,
) {
    val maxIndex = ((max - min) / step).toInt()
    val index = (((value.coerceIn(min, max) - min) / step).toInt()).coerceIn(0, maxIndex)
    SliderBlock(
        label = label,
        hint = hint,
        valueText = formatSize(value),
        value = index.toFloat(),
        valueRange = 0f..maxIndex.toFloat(),
        steps = 0,
        palette = palette,
        error = error,
        onValueChange = { next ->
            val nextIndex = next.roundToInt().coerceIn(0, maxIndex)
            onValueChange(min + (nextIndex.toLong() * step))
        },
    )
}

@Composable
private fun IntSliderRow(
    label: String,
    hint: String,
    value: Int,
    min: Int,
    max: Int,
    palette: HomePalette,
    onValueChange: (Int) -> Unit,
) {
    SliderBlock(
        label = label,
        hint = hint,
        valueText = value.toString(),
        value = value.coerceIn(min, max).toFloat(),
        valueRange = min.toFloat()..max.toFloat(),
        steps = (max - min - 1).coerceAtLeast(0),
        palette = palette,
        onValueChange = { next -> onValueChange(next.roundToInt().coerceIn(min, max)) },
    )
}

@Composable
private fun SliderBlock(
    label: String,
    hint: String,
    valueText: String,
    value: Float,
    valueRange: ClosedFloatingPointRange<Float>,
    steps: Int,
    palette: HomePalette,
    error: String? = null,
    onValueChange: (Float) -> Unit,
) {
    Column(
        modifier = Modifier
            .fillMaxWidth()
            .clip(RoundedCornerShape(14.dp))
            .background(palette.surfaceStrong)
            .border(1.dp, palette.border, RoundedCornerShape(14.dp))
            .padding(12.dp),
        verticalArrangement = Arrangement.spacedBy(4.dp),
    ) {
        Row(
            modifier = Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.SpaceBetween,
            verticalAlignment = Alignment.CenterVertically,
        ) {
            Text(
                text = label,
                color = palette.text,
                fontSize = 14.sp,
                fontWeight = FontWeight.SemiBold,
                modifier = Modifier.weight(1f),
            )
            Text(text = valueText, color = palette.textSoft, fontSize = 12.sp)
        }
        Slider(
            value = value,
            onValueChange = onValueChange,
            valueRange = valueRange,
            steps = steps,
            modifier = Modifier
                .fillMaxWidth()
                .padding(horizontal = 4.dp),
        )
        Text(text = hint, color = palette.textSoft, fontSize = 11.sp, lineHeight = 14.sp)
        if (error != null) {
            Text(text = error, color = Color(0xFFFF4D4F), fontSize = 11.sp, lineHeight = 14.sp)
        }
    }
}

@Composable
private fun ScreenShell(
    title: String,
    body: String,
    icon: ImageVector,
    accent: Color,
    onBack: () -> Unit,
    content: @Composable () -> Unit,
) {
    Box(
        modifier = Modifier
            .fillMaxSize()
            .background(Brush.linearGradient(listOf(Color(0xFFE8EEFC), Color(0xFFDBE7FF))))
            .safeDrawingPadding()
            .padding(horizontal = 12.dp, vertical = 10.dp),
    ) {
        Column(
            modifier = Modifier.fillMaxSize(),
            verticalArrangement = Arrangement.spacedBy(12.dp),
        ) {
            Row(
                modifier = Modifier
                    .fillMaxWidth()
                    .heightIn(min = 42.dp),
                verticalAlignment = Alignment.CenterVertically,
            ) {
                Box(
                    modifier = Modifier
                        .clip(RoundedCornerShape(12.dp))
                        .background(Color.White.copy(alpha = 0.78f))
                        .border(1.dp, Color(0x290F1D3A), RoundedCornerShape(12.dp))
                        .clickable(onClick = onBack)
                        .padding(horizontal = 12.dp, vertical = 9.dp),
                    contentAlignment = Alignment.Center,
                ) {
                    Icon(
                        imageVector = Icons.AutoMirrored.Rounded.ArrowBack,
                        contentDescription = "Back",
                        tint = Color(0xFF0F1D3A),
                        modifier = Modifier.size(20.dp),
                    )
                }
            }
            Column(
                modifier = Modifier.fillMaxWidth(),
                horizontalAlignment = Alignment.CenterHorizontally,
                verticalArrangement = Arrangement.spacedBy(8.dp),
            ) {
                Box(
                    modifier = Modifier
                        .size(72.dp)
                        .clip(RoundedCornerShape(20.dp))
                        .background(accent.copy(alpha = 0.14f))
                        .border(1.dp, accent.copy(alpha = 0.34f), RoundedCornerShape(20.dp)),
                    contentAlignment = Alignment.Center,
                ) {
                    Icon(
                        imageVector = icon,
                        contentDescription = null,
                        tint = accent,
                        modifier = Modifier.size(34.dp),
                    )
                }
                Text(
                    text = title,
                    color = Color(0xFF0F1D3A),
                    fontSize = 28.sp,
                    fontWeight = FontWeight.Bold,
                    textAlign = TextAlign.Center,
                )
                Text(
                    text = body,
                    color = Color(0xFF4D5D85),
                    fontSize = 15.sp,
                    textAlign = TextAlign.Center,
                )
            }
            Box(modifier = Modifier.weight(1f)) {
                content()
            }
        }
    }
}

@Composable
private fun ShellPanel(content: @Composable ColumnScope.() -> Unit) {
    Column(
        modifier = Modifier
            .fillMaxSize()
            .clip(RoundedCornerShape(22.dp))
            .background(Color.White.copy(alpha = 0.9f))
            .border(1.dp, Color(0x290F1D3A), RoundedCornerShape(22.dp))
            .padding(22.dp),
        horizontalAlignment = Alignment.CenterHorizontally,
        verticalArrangement = Arrangement.Center,
        content = content,
    )
}

@Composable
private fun VersionNote(palette: HomePalette) {
    Row(
        modifier = Modifier.fillMaxWidth(),
        horizontalArrangement = Arrangement.End,
    ) {
        Text(
            text = "0.4.2-beta   Made by infiniteplayapps",
            color = palette.textSoft,
            fontSize = 10.sp,
        )
    }
}

@Composable
private fun Pill(
    palette: HomePalette,
    modifier: Modifier = Modifier,
    content: @Composable () -> Unit,
) {
    Box(
        modifier = modifier
            .heightIn(min = 34.dp)
            .clip(RoundedCornerShape(999.dp))
            .background(palette.surfaceStrong)
            .border(1.dp, palette.border, RoundedCornerShape(999.dp))
            .padding(horizontal = 12.dp, vertical = 7.dp),
        contentAlignment = Alignment.Center,
    ) {
        content()
    }
}

private fun palette(themePreference: ThemePreference): HomePalette {
    return when (themePreference) {
        ThemePreference.Light -> HomePalette(
            bg = Color(0xFFE8EEFC),
            bgAccent = Color(0xFFDBE7FF),
            surface = Color.White.copy(alpha = 0.78f),
            surfaceStrong = Color.White.copy(alpha = 0.9f),
            border = Color(0x290F1D3A),
            text = Color(0xFF0F1D3A),
            textSoft = Color(0xFF3F4F74),
        )
        ThemePreference.Dark -> HomePalette(
            bg = Color(0xFF151926),
            bgAccent = Color(0xFF202A3F),
            surface = Color(0xFF252D3D),
            surfaceStrong = Color(0xFF1E2635),
            border = Color(0x4DFFFFFF),
            text = Color(0xFFF4F7FF),
            textSoft = Color(0xFFC1CADB),
        )
    }
}

@Composable
private fun thrufluxTypography(): Typography {
    val outfit = FontFamily(
        outfitFont(FontWeight.Normal, 560),
        outfitFont(FontWeight.Medium, 650),
        outfitFont(FontWeight.SemiBold, 730),
        outfitFont(FontWeight.Bold, 820),
        outfitFont(FontWeight.ExtraBold, 900),
    )
    val base = Typography()
    return Typography(
        displayLarge = base.displayLarge.copy(fontFamily = outfit, fontWeight = FontWeight.ExtraBold),
        displayMedium = base.displayMedium.copy(fontFamily = outfit, fontWeight = FontWeight.ExtraBold),
        displaySmall = base.displaySmall.copy(fontFamily = outfit, fontWeight = FontWeight.Bold),
        headlineLarge = base.headlineLarge.copy(fontFamily = outfit, fontWeight = FontWeight.Bold),
        headlineMedium = base.headlineMedium.copy(fontFamily = outfit, fontWeight = FontWeight.Bold),
        headlineSmall = base.headlineSmall.copy(fontFamily = outfit, fontWeight = FontWeight.Bold),
        titleLarge = base.titleLarge.copy(fontFamily = outfit, fontWeight = FontWeight.Bold),
        titleMedium = base.titleMedium.copy(fontFamily = outfit, fontWeight = FontWeight.Bold),
        titleSmall = base.titleSmall.copy(fontFamily = outfit, fontWeight = FontWeight.Bold),
        bodyLarge = base.bodyLarge.copy(fontFamily = outfit, fontWeight = FontWeight.SemiBold),
        bodyMedium = base.bodyMedium.copy(fontFamily = outfit, fontWeight = FontWeight.SemiBold),
        bodySmall = base.bodySmall.copy(fontFamily = outfit, fontWeight = FontWeight.SemiBold),
        labelLarge = base.labelLarge.copy(fontFamily = outfit, fontWeight = FontWeight.Bold),
        labelMedium = base.labelMedium.copy(fontFamily = outfit, fontWeight = FontWeight.Bold),
        labelSmall = base.labelSmall.copy(fontFamily = outfit, fontWeight = FontWeight.Bold),
    )
}

@OptIn(ExperimentalTextApi::class)
private fun outfitFont(weight: FontWeight, axisWeight: Int) = Font(
    resId = R.font.outfit,
    weight = weight,
    variationSettings = FontVariation.Settings(FontVariation.weight(axisWeight)),
)

private fun prefs(context: Context) = context.getSharedPreferences("thruflux_android", Context.MODE_PRIVATE)

private fun loadAppSettings(context: Context): AppSettings {
    val shared = prefs(context)
    return AppSettings(
        serverUrl = shared.getString("settings_server_url", DEFAULT_SERVER_URL) ?: DEFAULT_SERVER_URL,
        stunServer = shared.getString("settings_stun_server", DEFAULT_STUN_SERVER) ?: DEFAULT_STUN_SERVER,
        turnServers = shared.getString("settings_turn_servers", "") ?: "",
        forceTurn = shared.getBoolean("settings_force_turn", false),
        quicConnWindowBytes = shared.getLong("settings_qcw", DEFAULT_QCW).coerceIn(QCW_MIN, QCW_MAX),
        quicStreamWindowBytes = shared.getLong("settings_qsw", DEFAULT_QSW).coerceIn(QSW_MIN, QSW_MAX),
        overwrite = shared.getBoolean("settings_overwrite", false),
        udpBufferBytes = shared.getLong("settings_udp", DEFAULT_UDP).coerceIn(UDP_MIN, UDP_MAX),
        maxReceivers = shared.getInt("settings_max_receivers", DEFAULT_MAX_RECEIVERS).coerceIn(MAX_RECEIVERS_MIN, MAX_RECEIVERS_MAX),
        randomJoinCodeMode = shared.getBoolean("settings_random_join_code_mode", false),
        notifyReceiverSessionComplete = shared.getBoolean("settings_notify_receiver_session_complete", true),
        notifySenderReceiverJoined = shared.getBoolean("settings_notify_sender_receiver_joined", true),
        notifySenderReceiverComplete = shared.getBoolean("settings_notify_sender_receiver_complete", true),
        notifyTransferFailure = shared.getBoolean("settings_notify_transfer_failure", true),
    ).let {
        if (it.quicStreamWindowBytes > it.quicConnWindowBytes) {
            it.copy(quicStreamWindowBytes = it.quicConnWindowBytes)
        } else {
            it
        }
    }
}

private fun saveAppSettings(context: Context, settings: AppSettings) {
    prefs(context).edit()
        .putString("settings_server_url", settings.serverUrl)
        .putString("settings_stun_server", settings.stunServer)
        .putString("settings_turn_servers", settings.turnServers)
        .putBoolean("settings_force_turn", settings.forceTurn)
        .putLong("settings_qcw", settings.quicConnWindowBytes.coerceIn(QCW_MIN, QCW_MAX))
        .putLong("settings_qsw", settings.quicStreamWindowBytes.coerceIn(QSW_MIN, QSW_MAX))
        .putBoolean("settings_overwrite", settings.overwrite)
        .putLong("settings_udp", settings.udpBufferBytes.coerceIn(UDP_MIN, UDP_MAX))
        .putInt("settings_max_receivers", settings.maxReceivers.coerceIn(MAX_RECEIVERS_MIN, MAX_RECEIVERS_MAX))
        .putBoolean("settings_random_join_code_mode", settings.randomJoinCodeMode)
        .putBoolean("settings_notify_receiver_session_complete", settings.notifyReceiverSessionComplete)
        .putBoolean("settings_notify_sender_receiver_joined", settings.notifySenderReceiverJoined)
        .putBoolean("settings_notify_sender_receiver_complete", settings.notifySenderReceiverComplete)
        .putBoolean("settings_notify_transfer_failure", settings.notifyTransferFailure)
        .apply()
}

private fun validateSettings(settings: AppSettings): SettingsErrors {
    val turnEntries = settings.turnServers
        .split('\n', ',')
        .map { it.trim() }
        .filter { it.isNotEmpty() }
    return SettingsErrors(
        serverUrl = if (settings.serverUrl.trim().startsWith("ws://") || settings.serverUrl.trim().startsWith("wss://")) null else "Must start with ws:// or wss://",
        stunServer = if (settings.stunServer.trim().startsWith("stun://")) null else "Must start with stun://",
        turnServers = if (turnEntries.any { !it.startsWith("turn://") }) "Each entry must start with turn://" else null,
        quicRelation = if (settings.quicStreamWindowBytes <= settings.quicConnWindowBytes) null else "Stream window must be less than or equal to connection window",
    )
}

private fun loadThemePreference(context: Context): ThemePreference {
    return if (prefs(context).getString("theme", "light") == "dark") {
        ThemePreference.Dark
    } else {
        ThemePreference.Light
    }
}

private fun saveThemePreference(context: Context, preference: ThemePreference) {
    prefs(context).edit().putString("theme", preference.name.lowercase()).apply()
}

private fun loadDeviceCode(context: Context): String {
    val existing = prefs(context).getString("device_code", null)
    if (existing != null && existing.length == 19) {
        return existing
    }
    val generated = generateDeviceCode()
    saveDeviceCode(context, generated)
    return generated
}

private fun saveDeviceCode(context: Context, code: String) {
    prefs(context).edit().putString("device_code", code).apply()
}

private fun generateDeviceCode(): String {
    val alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    val random = SecureRandom()
    val raw = buildString {
        repeat(16) {
            append(alphabet[random.nextInt(alphabet.length)])
        }
    }
    return raw.chunked(4).joinToString("-")
}

private fun copyText(context: Context, label: String, text: String) {
    val clipboard = context.getSystemService(ClipboardManager::class.java)
    clipboard.setPrimaryClip(ClipData.newPlainText(label, text))
}

private fun shareText(context: Context, text: String) {
    val intent = Intent(Intent.ACTION_SEND)
        .setType("text/plain")
        .putExtra(Intent.EXTRA_TEXT, text)
    context.startActivity(Intent.createChooser(intent, "Share device code"))
}

private fun openUrl(context: Context, url: String) {
    val intent = Intent(Intent.ACTION_VIEW, Uri.parse(url))
        .addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
    context.startActivity(intent)
}

private fun hasStorageAccess(context: Context): Boolean {
    return if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.R) {
        Environment.isExternalStorageManager()
    } else {
        context.checkSelfPermission(Manifest.permission.READ_EXTERNAL_STORAGE) == PackageManager.PERMISSION_GRANTED
    }
}

private fun openManageStorageSettings(context: Context, launch: (Intent) -> Unit) {
    val intent = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.R) {
        Intent(AndroidSettings.ACTION_MANAGE_APP_ALL_FILES_ACCESS_PERMISSION).setData(Uri.parse("package:${context.packageName}"))
    } else {
        Intent(AndroidSettings.ACTION_APPLICATION_DETAILS_SETTINGS).setData(Uri.parse("package:${context.packageName}"))
    }
    launch(intent)
}

private fun mergeSendEntries(existing: List<SendEntry>, incoming: List<SendEntry>): List<SendEntry> {
    val seen = existing.map { "${it.isDirectory}:${it.path}" }.toMutableSet()
    val next = existing.toMutableList()
    incoming.forEach { entry ->
        val key = "${entry.isDirectory}:${entry.path}"
        if (seen.add(key)) {
            next.add(entry)
        }
    }
    return next
}

private fun buildHostPayload(entries: List<SendEntry>, settings: AppSettings, deviceCode: String): JSONObject {
    val payload = JSONObject()
        .put("paths", JSONArray(entries.map { it.path }))
        .put("serverUrl", settings.serverUrl)
        .put("maxReceivers", settings.maxReceivers)
        .put("stunServer", settings.stunServer)
        .put("turnServers", splitTurnServers(settings.turnServers).joinToString(","))
        .put("forceTurn", settings.forceTurn)
        .put("quicStreamWindowBytes", settings.quicStreamWindowBytes)
        .put("quicConnWindowBytes", settings.quicConnWindowBytes)
        .put("udpBufferBytes", settings.udpBufferBytes)
    if (!settings.randomJoinCodeMode) {
        payload.put("custom-join-code", deviceCode)
    }
    return payload
}

private fun buildReceivePayload(joinCode: String, out: String, settings: AppSettings): JSONObject {
    return JSONObject()
        .put("joinCode", normalizeJoinCode(joinCode))
        .put("out", out)
        .put("serverUrl", settings.serverUrl)
        .put("stunServer", settings.stunServer)
        .put("turnServers", splitTurnServers(settings.turnServers).joinToString(","))
        .put("forceTurn", settings.forceTurn)
        .put("quicConnWindowBytes", settings.quicConnWindowBytes)
        .put("quicStreamWindowBytes", settings.quicStreamWindowBytes)
        .put("overwrite", settings.overwrite)
        .put("udpBufferBytes", settings.udpBufferBytes)
}

private fun defaultReceiveDirectory(): String {
    return "$SHARED_STORAGE_ROOT/Download"
}

private fun commonPickerFolders(): List<QuickFolder> {
    return listOf(
        QuickFolder("Downloads", "$SHARED_STORAGE_ROOT/Download", Icons.Rounded.Download),
        QuickFolder("DCIM", "$SHARED_STORAGE_ROOT/DCIM", Icons.Rounded.CameraAlt),
        QuickFolder("Photos", "$SHARED_STORAGE_ROOT/Pictures", Icons.Rounded.InsertDriveFile),
        QuickFolder("Videos", "$SHARED_STORAGE_ROOT/Movies", Icons.Rounded.InsertDriveFile),
        QuickFolder("Music", "$SHARED_STORAGE_ROOT/Music", Icons.Rounded.Folder),
        QuickFolder("Documents", "$SHARED_STORAGE_ROOT/Documents", Icons.Rounded.InsertDriveFile),
        QuickFolder("Root", SHARED_STORAGE_ROOT, Icons.Rounded.Folder),
    )
}

private fun isValidJoinCode(value: String): Boolean {
    val trimmed = value.trim()
    val raw = trimmed.replace("-", "")
    if (raw.length != 16 || raw.any { !it.isLetterOrDigit() }) {
        return false
    }
    return trimmed.matches(Regex("^[A-Za-z0-9]{16}$")) ||
        trimmed.matches(Regex("^[A-Za-z0-9]{4}-[A-Za-z0-9]{4}-[A-Za-z0-9]{4}-[A-Za-z0-9]{4}$"))
}

private fun normalizeJoinCode(value: String): String {
    return value
        .trim()
        .replace("-", "")
        .uppercase(Locale.US)
        .chunked(4)
        .joinToString("-")
}

private fun loadSavedDevices(context: Context): List<SavedDevice> {
    val raw = prefs(context).getString("saved_devices", "[]") ?: "[]"
    return runCatching {
        val array = JSONArray(raw)
        buildList {
            for (index in 0 until array.length()) {
                val item = array.optJSONObject(index) ?: continue
                val code = item.optString("joinCode", "")
                if (isValidJoinCode(code)) {
                    add(
                        SavedDevice(
                            id = item.optString("id").ifBlank { item.optLong("updatedAt", System.currentTimeMillis()).toString() },
                            name = item.optString("name").ifBlank { "Saved device" },
                            joinCode = normalizeJoinCode(code),
                            createdAt = item.optLong("createdAt", System.currentTimeMillis()),
                            updatedAt = item.optLong("updatedAt", System.currentTimeMillis()),
                        ),
                    )
                }
            }
        }.sortedByDescending { it.updatedAt }
    }.getOrDefault(emptyList())
}

private fun saveSavedDevices(context: Context, devices: List<SavedDevice>) {
    val array = JSONArray()
    devices.forEach { device ->
        array.put(
            JSONObject()
                .put("id", device.id)
                .put("name", device.name)
                .put("joinCode", normalizeJoinCode(device.joinCode))
                .put("createdAt", device.createdAt)
                .put("updatedAt", device.updatedAt),
        )
    }
    prefs(context).edit().putString("saved_devices", array.toString()).apply()
}

private fun openFolder(context: Context, path: String, onError: (String) -> Unit) {
    val folder = File(path)
    if (!folder.exists() || !folder.isDirectory) {
        onError("Folder does not exist: $path")
        return
    }
    val intent = Intent(Intent.ACTION_VIEW)
        .setDataAndType(Uri.parse("file://${folder.absolutePath}"), "resource/folder")
        .addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
    runCatching {
        context.startActivity(intent)
    }.onFailure {
        onError("Open this folder manually: $path")
    }
}

private fun splitTurnServers(raw: String): List<String> {
    return raw
        .split('\n', ',')
        .map { it.trim() }
        .filter { it.isNotEmpty() }
}

private fun postJson(baseUrl: String, path: String, body: JSONObject?) {
    val connection = URL("$baseUrl$path").openConnection() as HttpURLConnection
    connection.connectTimeout = 2000
    connection.readTimeout = 2000
    connection.requestMethod = "POST"
    if (body != null) {
        val bytes = body.toString().toByteArray(Charsets.UTF_8)
        connection.doOutput = true
        connection.setRequestProperty("Content-Type", "application/json")
        connection.outputStream.use { it.write(bytes) }
    }
    val status = connection.responseCode
    if (status !in 200..299) {
        val text = runCatching {
            (connection.errorStream ?: connection.inputStream)?.bufferedReader()?.use { it.readText() }
        }.getOrNull().orEmpty()
        connection.disconnect()
        throw IllegalStateException(extractApiError(status, text))
    }
    connection.disconnect()
}

private fun extractApiError(status: Int, text: String): String {
    if (text.isBlank()) {
        return "Request failed with status $status"
    }
    return runCatching {
        JSONObject(text).optString("error").ifBlank { "Request failed with status $status" }
    }.getOrDefault(text)
}

private fun subscribeSenderEvents(
    baseUrl: String,
    onEvent: (ThrufluxEvent) -> Unit,
    onError: (String) -> Unit,
): Thread {
    val thread = Thread {
        var failures = 0
        while (!Thread.currentThread().isInterrupted) {
            val done = CountDownLatch(1)
            val lastError = AtomicReference<String?>(null)
            val sourceRef = AtomicReference<EventSource?>(null)
            val request = Request.Builder()
                .url("$baseUrl/events")
                .build()
            val listener = object : EventSourceListener() {
                override fun onEvent(eventSource: EventSource, id: String?, type: String?, data: String) {
                    failures = 0
                    parseEventLine(data.trim())?.let(onEvent)
                }

                override fun onClosed(eventSource: EventSource) {
                    done.countDown()
                }

                override fun onFailure(eventSource: EventSource, t: Throwable?, response: Response?) {
                    lastError.set(t?.message ?: response?.message ?: "Could not listen for updates")
                    done.countDown()
                }
            }
            sourceRef.set(eventSourceFactory.newEventSource(request, listener))
            try {
                done.await()
            } catch (_: InterruptedException) {
                sourceRef.get()?.cancel()
                Thread.currentThread().interrupt()
                return@Thread
            }
            if (Thread.currentThread().isInterrupted) {
                sourceRef.get()?.cancel()
                return@Thread
            }
            val message = lastError.get()
            if (message != null) {
                failures += 1
                if (failures >= 4) {
                    onError(message)
                    return@Thread
                }
            }
            Thread.sleep((500L * (failures + 1)).coerceAtMost(2000L))
        }
    }
    thread.start()
    return thread
}

private fun parseEventLine(payload: String): ThrufluxEvent? {
    return runCatching {
        val json = JSONObject(payload)
        val messageValue = json.opt("message")
        val message = when (messageValue) {
            is JSONObject -> messageValue
            is String -> messageValue.trim().takeIf { it.startsWith("{") }?.let { JSONObject(it) } ?: JSONObject()
            else -> if (json.has("receiverId") || json.has("receiver_id") || json.has("percent")) json else JSONObject()
        }
        ThrufluxEvent(type = json.optString("type"), message = message)
    }.getOrNull()
}

private val eventHttpClient: OkHttpClient = OkHttpClient.Builder()
    .connectTimeout(3, TimeUnit.SECONDS)
    .readTimeout(0, TimeUnit.MILLISECONDS)
    .retryOnConnectionFailure(true)
    .build()

private val eventSourceFactory = EventSources.createFactory(eventHttpClient)

private fun senderReceiverId(message: JSONObject, existing: List<SenderTransferProgress>): String? {
    val explicit = message.optStringAny(listOf("receiverId", "receiver_id", "receiverID", "id"), "")
    if (explicit.isNotBlank()) {
        return explicit
    }
    return when (existing.size) {
        0 -> "receiver-1"
        1 -> existing.first().receiverId
        else -> null
    }
}

private fun JSONObject.optStringAny(keys: List<String>, fallback: String): String {
    for (key in keys) {
        if (has(key) && !isNull(key)) {
            val value = optString(key, "").trim()
            if (value.isNotBlank()) {
                return value
            }
        }
    }
    return fallback
}

private fun JSONObject.optDoubleAny(keys: List<String>, fallback: Double): Double {
    for (key in keys) {
        if (has(key) && !isNull(key)) {
            return optDouble(key, fallback)
        }
    }
    return fallback
}

private fun JSONObject.optLongAny(keys: List<String>, fallback: Long): Long {
    for (key in keys) {
        if (has(key) && !isNull(key)) {
            return optLong(key, fallback)
        }
    }
    return fallback
}

private fun JSONObject.optIntAny(keys: List<String>, fallback: Int): Int {
    for (key in keys) {
        if (has(key) && !isNull(key)) {
            return optInt(key, fallback)
        }
    }
    return fallback
}

private fun JSONObject.optBooleanAny(keys: List<String>, fallback: Boolean): Boolean {
    for (key in keys) {
        if (has(key) && !isNull(key)) {
            return optBoolean(key, fallback)
        }
    }
    return fallback
}

private fun isNormalClosure(reason: String): Boolean {
    val normalized = reason.lowercase(Locale.US)
    return normalized.contains("normal") || normalized.contains("abort") || normalized.contains("closed")
}

private fun showTransferNotification(context: Context, title: String, body: String) {
    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU &&
        context.checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED
    ) {
        return
    }
    val manager = context.getSystemService(NotificationManager::class.java)
    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O && manager.getNotificationChannel(TRANSFER_NOTIFICATION_CHANNEL_ID) == null) {
        manager.createNotificationChannel(
            NotificationChannel(
                TRANSFER_NOTIFICATION_CHANNEL_ID,
                "Thruflux transfers",
                NotificationManager.IMPORTANCE_DEFAULT,
            ),
        )
    }
    val notification = android.app.Notification.Builder(context, TRANSFER_NOTIFICATION_CHANNEL_ID)
        .setSmallIcon(android.R.drawable.stat_sys_upload_done)
        .setContentTitle(title)
        .setContentText(body)
        .setContentIntent(appLaunchPendingIntent(context))
        .setAutoCancel(true)
        .build()
    manager.notify((System.currentTimeMillis() % Int.MAX_VALUE).toInt(), notification)
}

private fun appLaunchPendingIntent(context: Context): PendingIntent {
    val intent = Intent(context, MainActivity::class.java)
        .addFlags(Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_CLEAR_TOP or Intent.FLAG_ACTIVITY_SINGLE_TOP)
    return PendingIntent.getActivity(
        context,
        100,
        intent,
        PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT,
    )
}

private fun updateForegroundTransferNotification(context: Context, title: String, text: String, bigText: String) {
    val intent = Intent(context, EngineService::class.java)
        .setAction(EngineService.ACTION_UPDATE_TRANSFER)
        .putExtra(EngineService.EXTRA_NOTIFICATION_TITLE, title)
        .putExtra(EngineService.EXTRA_NOTIFICATION_TEXT, text)
        .putExtra(EngineService.EXTRA_NOTIFICATION_BIG_TEXT, bigText)
    context.startService(intent)
}

private fun clearForegroundTransferNotification(context: Context) {
    context.startService(Intent(context, EngineService::class.java).setAction(EngineService.ACTION_CLEAR_TRANSFER))
}

private fun senderNotificationText(transfers: List<SenderTransferProgress>): String {
    val active = transfers.filter { it.status == TransferStatus.Ongoing }
    val average = active.map { it.percent }.takeIf { it.isNotEmpty() }?.average() ?: 0.0
    val throughput = active.sumOf { it.ewmaThroughput }
    return "${average.roundToInt().coerceIn(0, 100)}% across ${active.size} receiver${if (active.size == 1) "" else "s"} • ${formatThroughput(throughput)}"
}

private fun senderNotificationBigText(transfers: List<SenderTransferProgress>): String {
    return transfers
        .filter { it.status == TransferStatus.Ongoing }
        .take(5)
        .joinToString("\n") {
            "${it.receiverId}: ${it.percent.roundToInt().coerceIn(0, 100)}%, ${formatSize(it.bytesMoved)} moved, ${formatThroughput(it.ewmaThroughput)}"
        }
        .ifBlank { "Waiting for active receivers" }
}

private fun receiverNotificationText(progress: ReceiveTransferProgress): String {
    return "${progress.percent.roundToInt().coerceIn(0, 100)}% • ${formatThroughput(progress.ewmaThroughput)} • ${formatSize(progress.bytesMoved)} downloaded"
}

private fun receiverNotificationBigText(progress: ReceiveTransferProgress): String {
    return "Progress ${progress.percent.roundToInt().coerceIn(0, 100)}%\nDownloaded ${formatSize(progress.bytesMoved)}\nSkipped ${formatSize(progress.skippedBytes)}\nFiles ${progress.filesMoved}/${progress.totalExpectedFilesCount}\nRoute ${if (progress.isRelayed) "Relayed" else "Direct"}"
}

private fun formatSize(size: Long): String {
    return when {
        size < 1024L -> "$size B"
        size < 1024L * 1024L -> "${(size / 1024.0).formatOne()} KB"
        size < 1024L * 1024L * 1024L -> "${(size / (1024.0 * 1024.0)).formatOne()} MB"
        else -> "${(size / (1024.0 * 1024.0 * 1024.0)).formatTwo()} GB"
    }
}

private fun Double.formatOne(): String = String.format("%.1f", this)

private fun Double.formatTwo(): String = String.format("%.2f", this)

private fun formatThroughput(bytesPerSecond: Double): String {
    return "${formatSize(bytesPerSecond.coerceAtLeast(0.0).roundToInt().toLong())}/s"
}

private fun formatEta(totalBytes: Long, movedBytes: Long, skippedBytes: Long, bytesPerSecond: Double): String {
    val done = (movedBytes + skippedBytes).coerceAtLeast(0L)
    val remaining = (totalBytes - done).coerceAtLeast(0L)
    if (remaining <= 0L) {
        return "0s"
    }
    if (!bytesPerSecond.isFinite() || bytesPerSecond <= 0.0) {
        return "Calculating..."
    }
    val seconds = (remaining / bytesPerSecond).roundToInt().coerceAtLeast(1)
    if (seconds < 60) {
        return "${seconds}s"
    }
    val minutes = seconds / 60
    val remSeconds = seconds % 60
    if (minutes < 60) {
        return if (remSeconds == 0) "${minutes}m" else "${minutes}m ${remSeconds}s"
    }
    val hours = minutes / 60
    val remMinutes = minutes % 60
    return if (remMinutes == 0) "${hours}h" else "${hours}h ${remMinutes}m"
}

private fun formatModifiedTime(value: Long): String {
    if (value <= 0L) {
        return "Unknown"
    }
    return SimpleDateFormat("yyyy-MM-dd HH:mm", Locale.getDefault()).format(Date(value))
}

private fun startEngineService(context: Context) {
    val intent = Intent(context, EngineService::class.java)
    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
        context.startForegroundService(intent)
    } else {
        context.startService(intent)
    }
}

private const val DEFAULT_SERVER_URL = "wss://bytepipe.app/ws"
private const val DEFAULT_STUN_SERVER = "stun://stun.cloudflare.com:3478"
private const val DESKTOP_DOWNLOAD_URL = "https://thruflux.bytepipe.app/"
private const val PRIVACY_POLICY_URL = "https://yielding-alibi-e88.notion.site/Privacy-Policy-Thruflux-368bfd7f3a4f80c289e3fa3391e6383e?source=copy_link"
private const val QSW_MIN = 256L * 1024L
private const val QSW_MAX = 2L * 1024L * 1024L * 1024L
private const val QCW_MIN = 1L * 1024L * 1024L
private const val QCW_MAX = 8L * 1024L * 1024L * 1024L
private const val UDP_MIN = 1L * 1024L * 1024L
private const val UDP_MAX = 16L * 1024L * 1024L
private const val MAX_RECEIVERS_MIN = 1
private const val MAX_RECEIVERS_MAX = 64
private const val DEFAULT_QCW = 268435456L
private const val DEFAULT_QSW = 33554432L
private const val DEFAULT_UDP = 8388608L
private const val DEFAULT_MAX_RECEIVERS = 10
private const val SHARED_STORAGE_ROOT = "/storage/emulated/0"
private const val TRANSFER_NOTIFICATION_CHANNEL_ID = "thruflux_transfers"

@Preview
@Composable
private fun ThrufluxAppPreview() {
    ThrufluxApp()
}

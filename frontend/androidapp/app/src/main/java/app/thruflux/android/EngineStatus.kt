package app.thruflux.android

import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow

enum class EngineState {
    Stopped,
    Starting,
    Ready,
    Failed,
}

object EngineStatus {
    private val stateFlow = MutableStateFlow(EngineState.Stopped)
    private val baseUrlFlow = MutableStateFlow<String?>(null)

    val state: StateFlow<EngineState> = stateFlow.asStateFlow()
    val baseUrl: StateFlow<String?> = baseUrlFlow.asStateFlow()

    fun setState(next: EngineState) {
        stateFlow.value = next
    }

    fun setBaseUrl(next: String?) {
        baseUrlFlow.value = next
    }
}

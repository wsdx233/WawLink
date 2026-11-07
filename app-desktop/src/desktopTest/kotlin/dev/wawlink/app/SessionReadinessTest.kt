package dev.wawlink.app

import dev.wawlink.protocol.AgentConfigurationContext
import dev.wawlink.protocol.AgentManagementState
import dev.wawlink.protocol.ManagedModel
import dev.wawlink.protocol.ManagedRole
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class SessionReadinessTest {
    @Test fun nativeAvailableBuiltinModelNeedsNoCustomProvider() {
        val builtin = ManagedModel("builtin", "model", available = true)
        val readiness = nativeModelReadiness(state(models = listOf(builtin), roles = listOf(role("builtin", "model"))))
        assertEquals(builtin, readiness.model)
        assertNull(readiness.error)
    }

    @Test fun exactRoleIdentityWinsOverCatalogueOrderAndDuplicateModelNames() {
        val first = ManagedModel("other", "same-id", available = true)
        val selected = ManagedModel("selected", "same-id", available = true)
        assertEquals(selected, nativeModelReadiness(state(models = listOf(first, selected), roles = listOf(role("selected", "same-id")))).model)
    }

    @Test fun hiddenOrUnresolvedDefaultNeverFallsBackToTheFirstModel() {
        val available = ManagedModel("provider", "available", available = true)
        val hidden = ManagedModel("provider", "bound", available = false)
        val hiddenDefault = nativeModelReadiness(state(models = listOf(available, hidden), roles = listOf(role("provider", "bound"))))
        assertNull(hiddenDefault.model)
        assertTrue(hiddenDefault.configureDefault)
        val missingIdentity = nativeModelReadiness(state(models = listOf(available), roles = listOf(ManagedRole("default", "默认模型", model = "provider/available"))))
        assertNull(missingIdentity.model)
        assertTrue(missingIdentity.configureDefault)
    }

    @Test fun missingAuthenticationAndNativeFailuresStayDistinct() {
        val unavailable = nativeModelReadiness(state(models = listOf(ManagedModel("provider", "model", available = false))))
        assertNull(unavailable.model)
        assertNull(unavailable.error)
        assertFalse(unavailable.configureDefault)
        val invalid = nativeModelReadiness(state().copy(modelsError = "config.yml 第 4 行无效"))
        assertNull(invalid.model)
        assertEquals("config.yml 第 4 行无效", invalid.error)
    }

    private fun role(provider: String, model: String) = ManagedRole("default", "默认模型", providerId = provider, modelId = model)
    private fun state(models: List<ManagedModel> = emptyList(), roles: List<ManagedRole> = emptyList()) = AgentManagementState(
        AgentConfigurationContext("container", "容器"), loading = false, modelsLoading = false, models = models, roles = roles,
    )
}

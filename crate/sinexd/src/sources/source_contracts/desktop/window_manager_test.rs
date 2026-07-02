use super::*;
use std::sync::{LazyLock, Mutex};
use xtask::sandbox::prelude::*;

static ENV_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

fn clear_hyprland_env() {
    unsafe {
        std::env::remove_var("SINEX_HYPRLAND_EVENT_SOCKET");
        std::env::remove_var("SINEX_HYPRLAND_RUNTIME_DIR");
        std::env::remove_var("XDG_RUNTIME_DIR");
        std::env::remove_var("SINEX_HYPRLAND_INSTANCE_SIGNATURE");
        std::env::remove_var("HYPRLAND_INSTANCE_SIGNATURE");
    }
}

#[sinex_test]
async fn baseline_adapter_config_prefers_explicit_event_socket() -> TestResult<()> {
    let _guard = ENV_LOCK.lock().expect("env lock poisoned");
    clear_hyprland_env();
    unsafe {
        std::env::set_var(
            "SINEX_HYPRLAND_EVENT_SOCKET",
            "/run/user/1000/hypr/explicit/.socket2.sock",
        );
        std::env::set_var("SINEX_HYPRLAND_RUNTIME_DIR", "/run/user/1000");
        std::env::set_var("SINEX_HYPRLAND_INSTANCE_SIGNATURE", "derived");
    }

    let config = <HyprlandParser as MaterialParser>::baseline_adapter_config();

    assert_eq!(
        config["socket_path"],
        "/run/user/1000/hypr/explicit/.socket2.sock"
    );
    assert_eq!(config["reconnect_on_eof"], true);
    clear_hyprland_env();
    Ok(())
}

#[sinex_test]
async fn baseline_adapter_config_derives_socket_from_bridge_env() -> TestResult<()> {
    let _guard = ENV_LOCK.lock().expect("env lock poisoned");
    clear_hyprland_env();
    unsafe {
        std::env::set_var("SINEX_HYPRLAND_RUNTIME_DIR", "/run/user/1000");
        std::env::set_var("SINEX_HYPRLAND_INSTANCE_SIGNATURE", "abc123");
    }

    let config = <HyprlandParser as MaterialParser>::baseline_adapter_config();

    assert_eq!(
        config["socket_path"],
        "/run/user/1000/hypr/abc123/.socket2.sock"
    );
    assert_eq!(config["reconnect_on_eof"], true);
    clear_hyprland_env();
    Ok(())
}

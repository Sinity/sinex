//! `sinex-session`: sources that need the operator's graphical session
//! (Hyprland socket and plugin, session D-Bus, clipboard, accessibility, MPRIS,
//! PipeWire audio, screen capture; home-directory sources). An outside
//! producer of `sinexd` over the NATS ingress (architecture §7.3).
fn main() {
    todo!(
        "architecture §7.3: session sources as LiveStream and Media drivers, publishing to the ingress"
    )
}

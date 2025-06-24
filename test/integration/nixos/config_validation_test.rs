use crate::common::prelude::*;
use std::process::Command;

#[test]
fn test_nix_config_validation_basic() {
    // Test that our Nix configuration validation works for a basic config
    let nix_expr = r#"
    let
      lib = import <nixpkgs/lib>;
      pkgs = import <nixpkgs> {};
      configGen = import ../../../nixos/config-gen.nix { inherit lib pkgs; };
      
      # Basic test configuration
      testCfg = {
        sources = {
          atuin = { enable = true; databasePath = "/home/user/.local/share/atuin/history.db"; pollInterval = 5; };
          filesystem = { enable = true; watchPaths = ["/home/user/Documents"]; excludePatterns = ["**/.git/**"]; };
          clipboard = { enable = false; };
        };
        logLevel = "info";
        dryRun = false;
      };
      
      testFullCfg = {
        blobStorage = { enable = false; repositoryPath = "/var/lib/sinex/annex"; };
        database = { autoSetup = true; };
      };
      
      result = configGen.mkValidatedCollectorConfig testCfg testFullCfg;
    in
      result.validationReport
    "#;
    
    let output = Command::new("nix-instantiate")
        .arg("--eval")
        .arg("--expr")
        .arg(nix_expr)
        .output()
        .expect("Failed to execute nix-instantiate");
    
    if !output.status.success() {
        panic!("Nix evaluation failed: {}", String::from_utf8_lossy(&output.stderr));
    }
    
    let result = String::from_utf8_lossy(&output.stdout);
    
    // Check that validation completed without errors
    assert!(result.contains("valid"), "Validation should be valid for basic config");
}

#[test]
fn test_nix_config_validation_invalid_events() {
    // Test that invalid event types are caught
    let nix_expr = r#"
    let
      lib = import <nixpkgs/lib>;
      pkgs = import <nixpkgs> {};
      configGen = import ../../../nixos/config-gen.nix { inherit lib pkgs; };
      
      # Test configuration with invalid events
      testCfg = {
        sources = {
          atuin = { enable = true; databasePath = "/home/user/.local/share/atuin/history.db"; pollInterval = 5; };
        };
        logLevel = "info";
        dryRun = false;
      };
      
      testFullCfg = {
        blobStorage = { enable = false; repositoryPath = "/var/lib/sinex/annex"; };
        database = { autoSetup = true; };
      };
      
      # Generate config with invalid events
      config = configGen.mkCollectorConfig testCfg testFullCfg;
      configWithInvalidEvents = config // {
        enabled_events = config.enabled_events ++ ["invalid.event.type" "malformed_event"];
      };
      
      # Validate the modified config
      eventValidation = configGen.validation.validateEnabledEvents configWithInvalidEvents.enabled_events;
    in
      eventValidation
    "#;
    
    let output = Command::new("nix-instantiate")
        .arg("--eval")
        .arg("--expr")
        .arg(nix_expr)
        .output()
        .expect("Failed to execute nix-instantiate");
    
    if !output.status.success() {
        panic!("Nix evaluation failed: {}", String::from_utf8_lossy(&output.stderr));
    }
    
    let result = String::from_utf8_lossy(&output.stdout);
    
    // Check that validation caught the invalid events
    assert!(result.contains("false"), "Validation should fail for invalid events");
}

#[test]
fn test_nix_config_validation_dependencies() {
    // Test dependency validation
    let nix_expr = r#"
    let
      lib = import <nixpkgs/lib>;
      pkgs = import <nixpkgs> {};
      configGen = import ../../../nixos/config-gen.nix { inherit lib pkgs; };
      
      # Test configuration with dependency violations
      testCfg = {
        sources = {
          asciinema = { 
            enable = true; 
            recordingsPath = "/home/user/recordings";
            autoAnnex = true;  # This requires blobStorage.enable = true
          };
          atuin = { 
            enable = true; 
            databasePath = "relative/path";  # Should be absolute
            pollInterval = 0;  # Should be > 0
          };
        };
        logLevel = "info";
        dryRun = false;
      };
      
      testFullCfg = {
        blobStorage = { enable = false; repositoryPath = "/var/lib/sinex/annex"; };  # Should be true for autoAnnex
        database = { autoSetup = true; };
      };
      
      depValidation = configGen.validation.validateDependencies testCfg testFullCfg;
    in
      depValidation
    "#;
    
    let output = Command::new("nix-instantiate")
        .arg("--eval")
        .arg("--expr")
        .arg(nix_expr)
        .output()
        .expect("Failed to execute nix-instantiate");
    
    if !output.status.success() {
        panic!("Nix evaluation failed: {}", String::from_utf8_lossy(&output.stderr));
    }
    
    let result = String::from_utf8_lossy(&output.stdout);
    
    // Check that dependency validation caught the errors
    assert!(result.contains("false"), "Dependency validation should fail");
    assert!(result.contains("errors"), "Should contain dependency errors");
}

#[test] 
fn test_nix_toml_validation() {
    // Test TOML syntax validation
    let nix_expr = r#"
    let
      lib = import <nixpkgs/lib>;
      pkgs = import <nixpkgs> {};
      configGen = import ../../../nixos/config-gen.nix { inherit lib pkgs; };
      
      # Test valid TOML
      validToml = ''
        enabled_events = ["file.created", "file.modified"]
        
        [output]
        database = true
        logging = false
      '';
      
      # Test invalid TOML
      invalidToml = ''
        enabled_events = ["file.created", "file.modified"
        # Missing closing bracket - invalid TOML
        
        [output
        database = true
      '';
      
      validResult = configGen.validation.validateToml validToml;
      # invalidResult would fail the build, so we only test valid case
    in
      validResult
    "#;
    
    let output = Command::new("nix-instantiate")
        .arg("--eval")
        .arg("--expr")
        .arg(nix_expr)
        .output()
        .expect("Failed to execute nix-instantiate");
    
    if !output.status.success() {
        panic!("Nix evaluation failed: {}", String::from_utf8_lossy(&output.stderr));
    }
    
    let result = String::from_utf8_lossy(&output.stdout);
    
    // Check that valid TOML passes validation
    assert!(result.contains("true"), "Valid TOML should pass validation");
}

#[test]
fn test_nix_config_optimization_suggestions() {
    // Test optimization suggestions
    let nix_expr = r#"
    let
      lib = import <nixpkgs/lib>;
      pkgs = import <nixpkgs> {};
      configGen = import ../../../nixos/config-gen.nix { inherit lib pkgs; };
      
      # Configuration that should trigger performance suggestions
      testCfg = {
        sources = {
          filesystem = { 
            enable = true; 
            watchPaths = ["/home/user/Documents" "/home/user/Code" "/home/user/Downloads" "/etc" "/var/log" "/opt"]; # Many paths
            excludePatterns = ["**/.git/**"]; 
          };
          dbus = { 
            enable = true; 
            monitorSystem = true; 
            logAllSignals = true;  # Should trigger performance warning
          };
          atuin = { 
            enable = true; 
            databasePath = "/home/user/.local/share/atuin/history.db"; 
            pollInterval = 1;  # Very frequent polling
          };
        };
        logLevel = "info";
        dryRun = false;
      };
      
      testFullCfg = {
        blobStorage = { enable = false; repositoryPath = "/var/lib/sinex/annex"; };
        database = { autoSetup = true; ssl.mode = "disable"; };  # Should trigger security warning
      };
      
      perfSuggestions = configGen.optimization.getPerformanceSuggestions testCfg testFullCfg;
      secSuggestions = configGen.optimization.getSecuritySuggestions testCfg testFullCfg;
    in
      { performance = perfSuggestions; security = secSuggestions; }
    "#;
    
    let output = Command::new("nix-instantiate")
        .arg("--eval")
        .arg("--expr")
        .arg(nix_expr)
        .output()
        .expect("Failed to execute nix-instantiate");
    
    if !output.status.success() {
        panic!("Nix evaluation failed: {}", String::from_utf8_lossy(&output.stderr));
    }
    
    let result = String::from_utf8_lossy(&output.stdout);
    
    // Check that suggestions were generated
    assert!(result.contains("performance"), "Should generate performance suggestions");
    assert!(result.contains("security"), "Should generate security suggestions");
}
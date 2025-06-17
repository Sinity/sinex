# Focused test for path resolution functionality only
# Tests path resolution without full NixOS module evaluation

let
  lib = import <nixpkgs/lib>;
  pkgs = import <nixpkgs> {};
  configGen = import ../../../nixos/config-gen.nix { inherit lib pkgs; };
  
  # Define pathUtils for testing (same as in full.nix)
  pathUtils = rec {
    resolvePath = path: 
      if lib.hasPrefix "~/" path then
        "/home/testuser/${lib.removePrefix "~/" path}"
      else if path == "~" then
        "/home/testuser"
      else if lib.hasPrefix "~" path then
        let
          userAndPath = lib.removePrefix "~" path;
          parts = lib.splitString "/" userAndPath;
          username = lib.head parts;
          remainingPath = lib.concatStringsSep "/" (lib.tail parts);
        in
          if remainingPath == "" then
            "/home/${username}"
          else
            "/home/${username}/${remainingPath}"
      else
        path;
    
    validateAbsolutePath = path:
      let resolved = resolvePath path;
      in lib.hasPrefix "/" resolved;
    
    getParentDir = path:
      let resolved = resolvePath path;
      in builtins.dirOf resolved;
    
    isPathSafe = path: allowedPrefixes:
      let 
        resolved = resolvePath path;
        normalizedPath = lib.removeSuffix "/" resolved;
      in
        lib.any (prefix: lib.hasPrefix (lib.removeSuffix "/" prefix) normalizedPath) allowedPrefixes;
    
    getAllUserPaths = cfg: lib.flatten [
      (lib.optional (cfg.sources.atuin.enable or false) cfg.sources.atuin.databasePath)
      (lib.optional (cfg.sources.shellHistory.enable or false) [
        cfg.sources.shellHistory.zshPath
        cfg.sources.shellHistory.bashPath
      ])
      (lib.optional (cfg.sources.asciinema.enable or false) cfg.sources.asciinema.recordingsPath)
      (lib.optional (cfg.sources.filesystem.enable or false) cfg.sources.filesystem.watchPaths)
    ];
    
    validateUserPathsSafety = cfg:
      let
        userPaths = getAllUserPaths cfg;
        homeDir = "/home/testuser";
        allowedPrefixes = [ homeDir "/tmp" ];
        unsafePaths = lib.filter (path: !(isPathSafe path allowedPrefixes)) userPaths;
      in {
        safe = (lib.length unsafePaths) == 0;
        unsafePaths = unsafePaths;
        allowedPrefixes = allowedPrefixes;
      };
  };
  
  # Test configuration
  testUnifiedCollectorConfig = {
    sources = {
      atuin = {
        enable = true;
        databasePath = "~/.local/share/atuin/history.db";
        pollInterval = 5;
      };
      shellHistory = {
        enable = true;
        zshPath = "~/.zsh_history";
        bashPath = "~/.bash_history";
      };
      asciinema = {
        enable = true;
        recordingsPath = "~/.local/share/asciinema";
        autoRecord = false;
        autoAnnex = true;
      };
      filesystem = {
        enable = true;
        watchPaths = [ "~/Documents" "~/Projects" "~/work/important" ];
        excludePatterns = [ "*.tmp" ".git/*" ];
      };
      kittyScrollback = {
        enable = true;
        socketPath = "/tmp/kitty";
        captureInterval = 15;
        maxScrollbackLines = 10000;
        captureOnCommand = true;
        commandCaptureDelay = 100;
      };
    };
    dryRun = false;
    logLevel = "info";
  };
  
  # Mock full configuration
  testFullConfig = {
    targetUser = "testuser";
    pathUtils = pathUtils;
    blobStorage = {
      enable = true;
      repositoryPath = "/var/lib/sinex/annex";
    };
  };
  
in rec {
  # Test 1: Basic Path Resolution
  basicPathResolution = {
    homeExpansion = pathUtils.resolvePath "~" == "/home/testuser";
    pathExpansion = pathUtils.resolvePath "~/.local/share/atuin/history.db" == 
                    "/home/testuser/.local/share/atuin/history.db";
    absoluteUnchanged = pathUtils.resolvePath "/absolute/path" == "/absolute/path";
    userPath = pathUtils.resolvePath "~otheruser/file" == "/home/otheruser/file";
    complexUserPath = pathUtils.resolvePath "~otheruser/deep/nested/path" == 
                      "/home/otheruser/deep/nested/path";
  };
  
  # Test 2: Path Validation
  pathValidation = {
    validatesAbsolute = pathUtils.validateAbsolutePath "/absolute/path";
    validatesTilde = pathUtils.validateAbsolutePath "~/relative/path";
    validatesUserTilde = pathUtils.validateAbsolutePath "~otheruser/path";
    validatesComplexTilde = pathUtils.validateAbsolutePath "~/.local/share/deep/nested/file";
  };
  
  # Test 3: Parent Directory Extraction
  parentDirectories = {
    simpleParent = pathUtils.getParentDir "~/file.txt" == "/home/testuser";
    nestedParent = pathUtils.getParentDir "~/.local/share/atuin/history.db" == 
                   "/home/testuser/.local/share/atuin";
    absoluteParent = pathUtils.getParentDir "/var/lib/file" == "/var/lib";
  };
  
  # Test 4: Configuration Generation with Path Resolution
  configurationGeneration = 
    let
      collectorConfig = configGen.mkCollectorConfig testUnifiedCollectorConfig testFullConfig;
    in {
      atuinPathResolved = 
        collectorConfig."event.shell_command_executed_atuin".db_path == 
        "/home/testuser/.local/share/atuin/history.db";
      
      shellHistoryResolved = 
        collectorConfig."event.shell_history_command".history_files == [
          "/home/testuser/.zsh_history"
          "/home/testuser/.bash_history"
        ];
      
      asciinemaResolved = 
        collectorConfig."event.terminal_asciinema".recordings_dir == 
        "/home/testuser/.local/share/asciinema";
      
      filesystemResolved = 
        collectorConfig."event.files".watch_patterns == [
          "/home/testuser/Documents"
          "/home/testuser/Projects"
          "/home/testuser/work/important"
        ];
      
      kittySocketUnchanged = 
        collectorConfig."event.terminal_scrollback".kitty_socket_path == "/tmp/kitty";
      
      gitAnnexCorrect = 
        collectorConfig."event.terminal_asciinema".git_annex_repo == testFullConfig.blobStorage.repositoryPath;
    };
  
  # Test 5: Path Safety Validation
  pathSafety = 
    let
      safety = pathUtils.validateUserPathsSafety testUnifiedCollectorConfig;
    in {
      allPathsSafe = safety.safe;
      correctAllowedPrefixes = safety.allowedPrefixes == [ "/home/testuser" "/tmp" ];
      noUnsafePaths = safety.unsafePaths == [];
    };
  
  # Test 6: User Path Collection
  userPathCollection = 
    let
      userPaths = pathUtils.getAllUserPaths testUnifiedCollectorConfig;
      expectedPaths = [
        "~/.local/share/atuin/history.db"
        "~/.zsh_history"
        "~/.bash_history"
        "~/.local/share/asciinema"
        "~/Documents"
        "~/Projects"
        "~/work/important"
      ];
    in {
      correctPathCount = (builtins.length userPaths) == (builtins.length expectedPaths);
      containsAtuinPath = builtins.elem "~/.local/share/atuin/history.db" userPaths;
      containsZshPath = builtins.elem "~/.zsh_history" userPaths;
      containsBashPath = builtins.elem "~/.bash_history" userPaths;
      containsAsciinemaPath = builtins.elem "~/.local/share/asciinema" userPaths;
      containsWatchPaths = 
        builtins.elem "~/Documents" userPaths &&
        builtins.elem "~/Projects" userPaths &&
        builtins.elem "~/work/important" userPaths;
    };
  
  # Test 7: Configuration Validation Integration
  configValidation = 
    let
      validation = configGen.validation.validateDependencies testUnifiedCollectorConfig testFullConfig;
    in {
      validationPasses = validation.valid;
      noValidationErrors = validation.errors == [];
    };
  
  # Collect all test results
  allTestResults = [
    basicPathResolution.homeExpansion
    basicPathResolution.pathExpansion
    basicPathResolution.absoluteUnchanged
    basicPathResolution.userPath
    basicPathResolution.complexUserPath
    pathValidation.validatesAbsolute
    pathValidation.validatesTilde
    pathValidation.validatesUserTilde
    pathValidation.validatesComplexTilde
    parentDirectories.simpleParent
    parentDirectories.nestedParent
    parentDirectories.absoluteParent
    configurationGeneration.atuinPathResolved
    configurationGeneration.shellHistoryResolved
    configurationGeneration.asciinemaResolved
    configurationGeneration.filesystemResolved
    configurationGeneration.kittySocketUnchanged
    configurationGeneration.gitAnnexCorrect
    pathSafety.allPathsSafe
    pathSafety.correctAllowedPrefixes
    pathSafety.noUnsafePaths
    userPathCollection.correctPathCount
    userPathCollection.containsAtuinPath
    userPathCollection.containsZshPath
    userPathCollection.containsBashPath
    userPathCollection.containsAsciinemaPath
    userPathCollection.containsWatchPaths
    configValidation.validationPasses
    configValidation.noValidationErrors
  ];
  
  # Summary
  summary = {
    total_tests = builtins.length allTestResults;
    passed_tests = builtins.length (builtins.filter (x: x) allTestResults);
    failed_tests = builtins.length (builtins.filter (x: !x) allTestResults);
    all_passed = builtins.all (x: x) allTestResults;
    pass_rate = "${toString (builtins.length (builtins.filter (x: x) allTestResults))}/${toString (builtins.length allTestResults)}";
  };
  
  # Debug info for failed tests
  debug = {
    inherit basicPathResolution pathValidation parentDirectories;
    inherit configurationGeneration pathSafety userPathCollection configValidation;
  };
}
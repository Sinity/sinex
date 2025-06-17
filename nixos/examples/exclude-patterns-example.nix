# Example configurations showing the improved exclude patterns ergonomics

{
  # Example 1: Most common use case - add custom excludes to sensible defaults
  services.sinex = {
    enable = true;
    preset = "normal";
    unifiedCollector.sources.filesystem = {
      # This adds to the 50+ sensible defaults, doesn't replace them
      excludePatterns = [
        "my-project/temp/*"
        "*.backup"
        "sensitive-dir/*"
      ];
    };
  };

  # Example 2: Advanced user wants complete control (rare)
  services.sinex = {
    enable = true;
    preset = "normal";
    unifiedCollector.sources.filesystem = {
      overrideDefaultExcludes = true;  # Disables all defaults
      excludePatterns = [
        # User must now specify ALL patterns they want
        ".git/*"
        "*.tmp"
        # ... their complete custom list
      ];
    };
  };

  # Example 3: Default behavior - just use the sensible defaults
  services.sinex = {
    enable = true;
    preset = "normal";
    # filesystem.excludePatterns is empty by default
    # So you get all the sensible defaults automatically
  };
}
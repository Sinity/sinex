# Health check configuration module - reduces repetition
{ lib, ... }:

with lib;

let
  # Common health check options generator
  mkHealthCheckOptions = {
    defaultPort,
    serviceName ? "",
    enableProbes ? true
  }: {
    enable = mkOption {
      type = types.bool;
      default = true;
      description = "Enable health checks for ${serviceName}";
    };

    port = mkOption {
      type = types.port;
      default = defaultPort;
      description = "HTTP port for health check endpoint";
    };

    path = mkOption {
      type = types.str;
      default = "/health";
      description = "HTTP path for health check endpoint";
    };

    readinessPath = mkOption {
      type = types.str;
      default = "/ready";
      description = "HTTP path for readiness probe endpoint";
    };

    livenessPath = mkOption {
      type = types.str;
      default = "/alive";
      description = "HTTP path for liveness probe endpoint";
    };

    interval = mkOption {
      type = types.int;
      default = 10;
      description = "Health check interval in seconds";
    };

    timeout = mkOption {
      type = types.int;
      default = 5;
      description = "Health check timeout in seconds";
    };
  } // optionalAttrs enableProbes {
    # Probe configurations (only if enabled)
    startupProbe = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Enable startup health probe";
      };

      initialDelay = mkOption {
        type = types.int;
        default = 30;
        description = "Initial delay before first startup probe in seconds";
      };

      periodSeconds = mkOption {
        type = types.int;
        default = 5;
        description = "Period between startup probes in seconds";
      };

      timeoutSeconds = mkOption {
        type = types.int;
        default = 3;
        description = "Timeout for startup probes in seconds";
      };

      failureThreshold = mkOption {
        type = types.int;
        default = 12;
        description = "Number of consecutive startup probe failures before giving up";
      };
    };

    readinessProbe = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Enable readiness health probe";
      };

      initialDelay = mkOption {
        type = types.int;
        default = 5;
        description = "Initial delay before first readiness probe in seconds";
      };

      periodSeconds = mkOption {
        type = types.int;
        default = 10;
        description = "Period between readiness probes in seconds";
      };

      timeoutSeconds = mkOption {
        type = types.int;
        default = 3;
        description = "Timeout for readiness probes in seconds";
      };

      failureThreshold = mkOption {
        type = types.int;
        default = 3;
        description = "Number of consecutive readiness probe failures before marking unready";
      };
    };

    livenessProbe = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Enable liveness health probe";
      };

      initialDelay = mkOption {
        type = types.int;
        default = 60;
        description = "Initial delay before first liveness probe in seconds";
      };

      periodSeconds = mkOption {
        type = types.int;
        default = 30;
        description = "Period between liveness probes in seconds";
      };

      timeoutSeconds = mkOption {
        type = types.int;
        default = 5;
        description = "Timeout for liveness probes in seconds";
      };

      failureThreshold = mkOption {
        type = types.int;
        default = 3;
        description = "Number of consecutive liveness probe failures before restart";
      };
    };
  };

  # Advanced health check options for detailed customization
  mkAdvancedHealthCheckOptions = {
    defaultPort,
    serviceName ? "",
    enableProbes ? true,
    enableMetrics ? true
  }: mkHealthCheckOptions { inherit defaultPort serviceName enableProbes; } // optionalAttrs enableMetrics {
    # Metrics collection settings
    metricsCollection = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Enable detailed metrics collection for ${serviceName}";
      };

      responseTimeHistogram = mkOption {
        type = types.bool;
        default = true;
        description = "Collect response time histograms";
      };

      errorRateTracking = mkOption {
        type = types.bool;
        default = true;
        description = "Track error rates and categorize by type";
      };

      customLabels = mkOption {
        type = types.attrsOf types.str;
        default = {};
        description = "Custom metric labels for ${serviceName}";
      };
    };

    # Alert thresholds
    alerting = {
      enable = mkOption {
        type = types.bool;
        default = false;
        description = "Enable alerting for ${serviceName}";
      };

      errorRateThreshold = mkOption {
        type = types.float;
        default = 0.05;
        description = "Error rate threshold (0.05 = 5%) before alerting";
      };

      responseTimeThreshold = mkOption {
        type = types.int;
        default = 5000;
        description = "Response time threshold in milliseconds before alerting";
      };

      uptimeThreshold = mkOption {
        type = types.float;
        default = 0.99;
        description = "Uptime threshold (0.99 = 99%) before alerting";
      };
    };
  };

  # Common restart policy options
  mkRestartOptions = {
    policy = mkOption {
      type = types.enum [ "always" "on-failure" "unless-stopped" "no" ];
      default = "always";
      description = "Restart policy for the service";
    };

    maxRestarts = mkOption {
      type = types.int;
      default = 5;
      description = "Maximum number of restarts within restart window";
    };

    restartWindow = mkOption {
      type = types.str;
      default = "10min";
      description = "Time window for counting restarts";
    };

    baseDelay = mkOption {
      type = types.str;
      default = "5s";
      description = "Base delay for restart backoff";
    };

    maxDelay = mkOption {
      type = types.str;
      default = "1min";
      description = "Maximum delay for restart backoff";
    };

    backoffMultiplier = mkOption {
      type = types.float;
      default = 2.0;
      description = "Backoff multiplier for exponential restart delay";
    };
  };

in
{
  # Export utility functions
  inherit mkHealthCheckOptions mkAdvancedHealthCheckOptions mkRestartOptions;
}
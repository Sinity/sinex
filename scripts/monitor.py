#!/usr/bin/env python3
"""
Simple monitoring dashboard for Sinex
Queries the health aggregator API and displays alerts
"""

import requests
import json
import time
import sys
from datetime import datetime
from typing import Dict, Any, Optional
import argparse

class SinexMonitor:
    def __init__(self, health_url: str = "http://localhost:8082"):
        self.health_url = health_url.rstrip('/')
        
    def get_system_health(self) -> Optional[Dict[str, Any]]:
        """Get overall system health"""
        try:
            response = requests.get(f"{self.health_url}/system", timeout=5)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"❌ Failed to get system health: {e}")
            return None
    
    def get_alerts(self) -> Optional[Dict[str, Any]]:
        """Get monitoring alerts"""
        try:
            response = requests.get(f"{self.health_url}/alerts", timeout=5)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"❌ Failed to get alerts: {e}")
            return None
    
    def print_system_status(self, health: Dict[str, Any]) -> None:
        """Print formatted system status"""
        status = health.get("overall_status", "unknown")
        summary = health.get("system_summary", {})
        
        # Status emoji
        status_emoji = {
            "healthy": "✅",
            "degraded": "⚠️",
            "failed": "❌",
            "unknown": "❓"
        }.get(status, "❓")
        
        print(f"\n{status_emoji} System Status: {status.upper()}")
        print(f"📊 Components: {summary.get('healthy_components', 0)}✅ {summary.get('degraded_components', 0)}⚠️ {summary.get('failed_components', 0)}❌")
        
        # Component details
        components = health.get("components", {})
        if components:
            print("\n📋 Component Details:")
            for name, comp in components.items():
                comp_status = comp.get("status", "unknown")
                comp_emoji = {
                    "healthy": "✅",
                    "degraded": "⚠️", 
                    "failed": "❌"
                }.get(comp_status, "❓")
                
                memory_mb = comp.get("memory_usage_mb", 0)
                events_min = comp.get("events_processed_last_minute", 0)
                last_hb = comp.get("time_since_last_heartbeat_seconds", 0)
                
                print(f"  {comp_emoji} {name:<25} {memory_mb:>4}MB  {events_min:>4}evt/min  {last_hb:>3}s ago")
    
    def print_alerts(self, alerts: Dict[str, Any]) -> None:
        """Print formatted alerts"""
        alert_data = alerts.get("alerts", {})
        
        # Silent sources
        silent_sources = alert_data.get("silent_sources", [])
        if silent_sources:
            print(f"\n🔇 Silent Sources Detected:")
            for alert in silent_sources:
                sources = alert.get("silent_sources", [])
                threshold = alert.get("threshold_minutes", 5)
                print(f"  ⚠️ {len(sources)} sources silent for >{threshold}min: {', '.join(sources)}")
        
        # Resource exhaustion
        resource_alerts = alert_data.get("resource_exhaustion", [])
        if resource_alerts:
            print(f"\n💾 Resource Exhaustion Alerts:")
            for alert in resource_alerts:
                status = alert.get("status", "unknown")
                warnings = alert.get("warnings", [])
                criticals = alert.get("criticals", [])
                
                if criticals:
                    for crit in criticals:
                        print(f"  🚨 CRITICAL: {crit}")
                if warnings:
                    for warn in warnings:
                        print(f"  ⚠️ WARNING: {warn}")
        
        # Schema failures
        schema_failures = alert_data.get("schema_failures", {})
        failure_count = schema_failures.get("count_last_hour", 0)
        if failure_count > 0:
            failing_sources = schema_failures.get("failing_sources", [])
            print(f"\n📋 Schema Validation Issues:")
            print(f"  ❌ {failure_count} failures in last hour")
            if failing_sources:
                print(f"  📝 Failing sources: {', '.join(failing_sources)}")
    
    def monitor_once(self) -> bool:
        """Run monitoring check once, return True if all healthy"""
        print(f"🔍 Monitoring Sinex @ {datetime.now().strftime('%H:%M:%S')}")
        
        # Get system health
        health = self.get_system_health()
        if not health:
            return False
        
        self.print_system_status(health)
        
        # Get alerts
        alerts = self.get_alerts()
        if alerts:
            alert_data = alerts.get("alerts", {})
            has_alerts = (
                alert_data.get("silent_sources") or 
                alert_data.get("resource_exhaustion") or
                alert_data.get("schema_failures", {}).get("count_last_hour", 0) > 0
            )
            
            if has_alerts:
                self.print_alerts(alerts)
            else:
                print("\n✅ No active alerts")
        
        overall_status = health.get("overall_status", "unknown")
        return overall_status == "healthy"
    
    def monitor_continuous(self, interval: int = 30) -> None:
        """Continuously monitor with specified interval"""
        print(f"🔄 Starting continuous monitoring (every {interval}s)")
        print("Press Ctrl+C to stop")
        
        try:
            while True:
                healthy = self.monitor_once()
                if not healthy:
                    print("⚠️ System not healthy - consider investigating")
                
                print(f"\n{'='*60}")
                time.sleep(interval)
        except KeyboardInterrupt:
            print("\n👋 Monitoring stopped")

def main():
    parser = argparse.ArgumentParser(description="Sinex monitoring dashboard")
    parser.add_argument("--url", default="http://localhost:8082", 
                       help="Health aggregator URL (default: http://localhost:8082)")
    parser.add_argument("--watch", "-w", action="store_true",
                       help="Continuous monitoring mode")
    parser.add_argument("--interval", "-i", type=int, default=30,
                       help="Monitoring interval in seconds (default: 30)")
    parser.add_argument("--alerts-only", "-a", action="store_true",
                       help="Show only alerts, skip system status")
    
    args = parser.parse_args()
    
    monitor = SinexMonitor(args.url)
    
    if args.watch:
        monitor.monitor_continuous(args.interval)
    else:
        healthy = monitor.monitor_once()
        sys.exit(0 if healthy else 1)

if __name__ == "__main__":
    main()
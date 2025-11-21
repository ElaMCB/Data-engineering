"""
Cost Optimization Utilities
Provides strategies for reducing AWS EMR costs
"""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass
import structlog

logger = structlog.get_logger()


@dataclass
class InstanceConfig:
    """Instance configuration for EMR cluster"""
    instance_type: str
    instance_count: int
    market_type: str  # "ON_DEMAND" or "SPOT"
    bid_price: Optional[float] = None


@dataclass
class AutoScalingConfig:
    """Auto-scaling configuration"""
    min_instances: int
    max_instances: int
    scale_up_threshold: float = 0.75  # CPU utilization
    scale_down_threshold: float = 0.25
    scale_up_cooldown: int = 300  # seconds
    scale_down_cooldown: int = 600  # seconds


class CostOptimizer:
    """
    Provides cost optimization strategies for AWS EMR clusters
    
    Features:
    - Spot instance recommendations
    - Auto-scaling configuration
    - Instance type optimization
    - Cost estimation
    """
    
    def __init__(
        self,
        use_spot_instances: bool = True,
        auto_scaling: bool = True,
        min_instances: int = 2,
        max_instances: int = 20,
        spot_bid_percentage: float = 0.70  # 70% of on-demand price
    ):
        """
        Initialize Cost Optimizer
        
        Args:
            use_spot_instances: Enable spot instances
            auto_scaling: Enable auto-scaling
            min_instances: Minimum cluster size
            max_instances: Maximum cluster size
            spot_bid_percentage: Spot bid as percentage of on-demand price
        """
        self.use_spot_instances = use_spot_instances
        self.auto_scaling = auto_scaling
        self.min_instances = min_instances
        self.max_instances = max_instances
        self.spot_bid_percentage = spot_bid_percentage
        self.logger = logger.bind(component="CostOptimizer")
    
    def get_emr_config(
        self,
        instance_type: str = "m5.xlarge",
        master_instance_type: Optional[str] = None,
        core_instance_count: int = 2,
        task_instance_count: int = 0
    ) -> Dict[str, Any]:
        """
        Generate optimized EMR cluster configuration
        
        Args:
            instance_type: Instance type for core/task nodes
            master_instance_type: Instance type for master node
            core_instance_count: Number of core instances
            task_instance_count: Number of task instances
            
        Returns:
            EMR configuration dictionary
        """
        self.logger.info("Generating EMR configuration", instance_type=instance_type)
        
        master_instance_type = master_instance_type or instance_type
        
        # Master instance (always on-demand for reliability)
        master_config = {
            "InstanceType": master_instance_type,
            "InstanceCount": 1,
            "Market": "ON_DEMAND"
        }
        
        # Core instances
        core_market = "SPOT" if self.use_spot_instances else "ON_DEMAND"
        core_config = {
            "InstanceType": instance_type,
            "InstanceCount": core_instance_count,
            "Market": core_market
        }
        
        if core_market == "SPOT":
            # Calculate spot bid price (would need EC2 pricing API in production)
            core_config["BidPrice"] = self._calculate_spot_bid(instance_type)
        
        # Task instances (if using auto-scaling)
        task_configs = []
        if task_instance_count > 0 or self.auto_scaling:
            task_market = "SPOT" if self.use_spot_instances else "ON_DEMAND"
            task_config = {
                "InstanceType": instance_type,
                "InstanceCount": task_instance_count if not self.auto_scaling else 0,
                "Market": task_market
            }
            
            if task_market == "SPOT":
                task_config["BidPrice"] = self._calculate_spot_bid(instance_type)
            
            task_configs.append(task_config)
        
        config = {
            "MasterInstanceGroup": master_config,
            "CoreInstanceGroup": core_config,
            "TaskInstanceGroups": task_configs if task_configs else None,
            "AutoScaling": self._get_autoscaling_config() if self.auto_scaling else None
        }
        
        return config
    
    def _calculate_spot_bid(self, instance_type: str) -> str:
        """
        Calculate spot bid price (simplified - would use EC2 pricing API in production)
        
        Args:
            instance_type: EC2 instance type
            
        Returns:
            Spot bid price as string
        """
        # Simplified calculation - in production, query EC2 pricing API
        # For now, return a placeholder that represents 70% of typical on-demand price
        # This would be replaced with actual pricing lookup
        base_prices = {
            "m5.xlarge": 0.192,
            "m5.2xlarge": 0.384,
            "m5.4xlarge": 0.768,
            "r5.xlarge": 0.252,
            "r5.2xlarge": 0.504,
            "c5.xlarge": 0.17,
            "c5.2xlarge": 0.34,
        }
        
        base_price = base_prices.get(instance_type, 0.20)
        spot_price = base_price * self.spot_bid_percentage
        
        return f"{spot_price:.3f}"
    
    def _get_autoscaling_config(self) -> Dict[str, Any]:
        """
        Generate auto-scaling configuration
        
        Returns:
            Auto-scaling configuration dictionary
        """
        return {
            "MinInstances": self.min_instances,
            "MaxInstances": self.max_instances,
            "ScaleUpPolicy": {
                "AdjustmentType": "CHANGE_IN_CAPACITY",
                "ScalingAdjustment": 2,
                "Cooldown": 300,
                "MetricSpecification": {
                    "PredefinedMetricSpecification": {
                        "PredefinedMetricType": "ASGAverageCPUUtilization"
                    },
                    "TargetValue": 75.0
                }
            },
            "ScaleDownPolicy": {
                "AdjustmentType": "CHANGE_IN_CAPACITY",
                "ScalingAdjustment": -1,
                "Cooldown": 600,
                "MetricSpecification": {
                    "PredefinedMetricSpecification": {
                        "PredefinedMetricType": "ASGAverageCPUUtilization"
                    },
                    "TargetValue": 25.0
                }
            }
        }
    
    def estimate_cost(
        self,
        instance_type: str,
        instance_count: int,
        runtime_hours: float,
        use_spot: bool = True
    ) -> Dict[str, float]:
        """
        Estimate cluster cost
        
        Args:
            instance_type: Instance type
            instance_count: Number of instances
            runtime_hours: Expected runtime in hours
            use_spot: Whether using spot instances
            
        Returns:
            Cost breakdown dictionary
        """
        # Simplified cost estimation (would use actual pricing in production)
        base_prices = {
            "m5.xlarge": 0.192,
            "m5.2xlarge": 0.384,
            "m5.4xlarge": 0.768,
        }
        
        hourly_rate = base_prices.get(instance_type, 0.20)
        
        if use_spot:
            hourly_rate *= self.spot_bid_percentage
        
        total_cost = hourly_rate * instance_count * runtime_hours
        
        return {
            "hourly_rate_per_instance": hourly_rate,
            "instance_count": instance_count,
            "runtime_hours": runtime_hours,
            "total_cost": total_cost,
            "estimated_savings_vs_ondemand": (
                base_prices.get(instance_type, 0.20) * instance_count * runtime_hours - total_cost
            ) if use_spot else 0
        }
    
    def get_optimization_recommendations(
        self,
        current_config: Dict[str, Any],
        workload_characteristics: Dict[str, Any]
    ) -> List[str]:
        """
        Get cost optimization recommendations
        
        Args:
            current_config: Current EMR configuration
            workload_characteristics: Workload characteristics (CPU-bound, memory-bound, etc.)
            
        Returns:
            List of recommendation strings
        """
        recommendations = []
        
        if not self.use_spot_instances:
            recommendations.append(
                "Enable spot instances for task nodes to reduce costs by up to 70%"
            )
        
        if not self.auto_scaling:
            recommendations.append(
                "Enable auto-scaling to scale down during low utilization periods"
            )
        
        if workload_characteristics.get("interrupt_tolerant", True):
            recommendations.append(
                "Workload is interrupt-tolerant - consider using spot instances for core nodes"
            )
        
        if current_config.get("CoreInstanceGroup", {}).get("InstanceCount", 0) > 10:
            recommendations.append(
                "Consider using larger instance types (e.g., 2xlarge) to reduce instance count"
            )
        
        return recommendations


"""
Common helper functions for ClickHouse Helm chart tests.

This module provides simple helper functions to reduce code duplication
in test scenarios.
"""

from testflows.core import *
import tests.steps.kubernetes as kubernetes
import tests.steps.clickhouse as clickhouse
import yaml
import json
import os


@TestStep(Then)
def wait_for_clickhouse_deployment(self, namespace: str, expected_pod_count: int = 2, expected_clickhouse_count: int = None):
    """Wait for ClickHouse deployment to be ready with all pods running.
    
    This is a common pattern used across most test scenarios:
    1. Wait for expected number of pods to be created
    2. Wait for all pods to be running
    3. Wait for ClickHouse pods specifically to be running
    
    Args:
        namespace: Kubernetes namespace
        expected_pod_count: Total number of pods expected (default: 2)
        expected_clickhouse_count: Number of ClickHouse pods expected (default: same as total)
    """
    if expected_clickhouse_count is None:
        expected_clickhouse_count = expected_pod_count
    
    with When(f"wait for {expected_pod_count} pods to be created"):
        kubernetes.wait_for_pod_count(namespace=namespace, expected_count=expected_pod_count)

    with And("wait for all pods to be running"):
        pods = kubernetes.wait_for_pods_running(namespace=namespace)
        note(f"All {len(pods)} pods are now running and ready")

    with And("wait for ClickHouse pods to be running"):
        clickhouse_pods = clickhouse.wait_for_clickhouse_pods_running(
            namespace=namespace, 
            expected_count=expected_clickhouse_count
        )
        note(f"ClickHouse pods running: {clickhouse_pods}")


class HelmState:
    """Represents the expected state of a Helm deployment based on values file.
    
    This class loads a Helm values configuration and provides methods to verify
    that the actual deployment matches the expected configuration.
    """
    
    def __init__(self, values_file_path):
        """Initialize HelmState with a values file.
        
        Args:
            values_file_path: Path to the Helm values YAML file
        """
        with open(values_file_path, 'r') as f:
            self.values = yaml.safe_load(f)
        
        self.values_file_path = values_file_path
        
    def get_expected_pod_count(self):
        """Calculate total expected pod count based on configuration."""
        clickhouse_count = self.get_expected_clickhouse_pod_count()
        keeper_enabled = self.values.get('keeper', {}).get('enabled', False)
        keeper_count = self.values.get('keeper', {}).get('replicaCount', 0) if keeper_enabled else 0
        return clickhouse_count + keeper_count
    
    def get_expected_clickhouse_pod_count(self):
        """Calculate expected ClickHouse pod count (replicas * shards)."""
        replicas = self.values.get('clickhouse', {}).get('replicasCount', 1)
        shards = self.values.get('clickhouse', {}).get('shardsCount', 1)
        return replicas * shards
    
    def get_expected_keeper_count(self):
        """Get expected Keeper pod count."""
        keeper_enabled = self.values.get('keeper', {}).get('enabled', False)
        return self.values.get('keeper', {}).get('replicaCount', 0) if keeper_enabled else 0
    
    def verify_deployment(self, namespace):
        """Verify the deployment matches expected configuration."""
        expected_total = self.get_expected_pod_count()
        expected_ch = self.get_expected_clickhouse_pod_count()
        expected_keeper = self.get_expected_keeper_count()
        
        note(f"Expected pods - Total: {expected_total}, ClickHouse: {expected_ch}, Keeper: {expected_keeper}")
        
        wait_for_clickhouse_deployment(
            namespace=namespace,
            expected_pod_count=expected_total,
            expected_clickhouse_count=expected_ch
        )
        
        self.verify_pod_counts(namespace=namespace)
    
    def verify_pod_counts(self, namespace):
        """Verify correct number of pods are running."""
        clickhouse_pods = clickhouse.get_clickhouse_pods(namespace=namespace)
        expected_ch_count = self.get_expected_clickhouse_pod_count()
        assert len(clickhouse_pods) == expected_ch_count, \
            f"Expected {expected_ch_count} ClickHouse pods, got {len(clickhouse_pods)}"
        note(f"✓ ClickHouse pods: {len(clickhouse_pods)}/{expected_ch_count}")
        
        expected_keeper_count = self.get_expected_keeper_count()
        if expected_keeper_count > 0:
            keeper_pods = kubernetes.run(
                cmd=f"kubectl get pods -n {namespace} -l clickhouse-keeper.altinity.com/app=chop -o jsonpath='{{.items[*].metadata.name}}'"
            )
            keeper_pod_count = len(keeper_pods.stdout.split()) if keeper_pods.stdout else 0
            assert keeper_pod_count == expected_keeper_count, \
                f"Expected {expected_keeper_count} Keeper pods, got {keeper_pod_count}"
            note(f"✓ Keeper pods: {keeper_pod_count}/{expected_keeper_count}")
    
    def verify_name_override(self, namespace):
        """Verify custom name is used if nameOverride is set."""
        name_override = self.values.get('nameOverride')
        if name_override:
            clickhouse.verify_custom_name_in_resources(
                namespace=namespace, 
                custom_name=name_override
            )
            note(f"✓ nameOverride: {name_override}")
    
    def verify_persistence(self, namespace):
        """Verify persistence configuration if enabled."""
        persistence_config = self.values.get('clickhouse', {}).get('persistence', {})
        if persistence_config.get('enabled', False):
            expected_size = persistence_config.get('size')
            
            clickhouse.verify_persistence_configuration(
                namespace=namespace, 
                expected_size=expected_size
            )
            
            pvcs = kubernetes.get_pvcs(namespace=namespace)
            assert len(pvcs) > 0, "No PVCs found for persistence"
            
            size_found = False
            for pvc in pvcs:
                if "clickhouse" in pvc.lower() or "chi-" in pvc.lower():
                    pvc_info = kubernetes.run(
                        cmd=f"kubectl get pvc {pvc} -n {namespace} -o json"
                    )
                    pvc_data = json.loads(pvc_info.stdout)
                    storage_size = (
                        pvc_data.get("spec", {})
                        .get("resources", {})
                        .get("requests", {})
                        .get("storage")
                    )
                    if storage_size == expected_size:
                        size_found = True
                        note(f"✓ Persistence: {storage_size}")
                        break
            
            assert size_found, f"No PVC found with expected storage size {expected_size}"
    
    def verify_service(self, namespace):
        """Verify LoadBalancer service if enabled."""
        lb_config = self.values.get('clickhouse', {}).get('lbService', {})
        if lb_config.get('enabled', False):
            services = kubernetes.get_services(namespace=namespace)
            lb_services = [
                s for s in services
                if kubernetes.get_service_type(service_name=s, namespace=namespace) == "LoadBalancer"
            ]
            assert len(lb_services) > 0, "LoadBalancer service not found"
            
            lb_service_name = lb_services[0]
            service_info = kubernetes.get_service_info(
                service_name=lb_service_name, 
                namespace=namespace
            )
            
            expected_ranges = lb_config.get('loadBalancerSourceRanges', [])
            if expected_ranges:
                source_ranges = service_info["spec"].get("loadBalancerSourceRanges", [])
                assert source_ranges == expected_ranges, \
                    f"Expected source ranges {expected_ranges}, got {source_ranges}"
            
            note(f"✓ LoadBalancer service: {lb_service_name}")
    
    def verify_users(self, namespace):
        """Verify user configuration and connectivity."""
        default_user_config = self.values.get('clickhouse', {}).get('defaultUser', {})
        users_config = self.values.get('clickhouse', {}).get('users', [])
        
        clickhouse_pods = clickhouse.get_clickhouse_pods(namespace=namespace)
        if not clickhouse_pods:
            return
        
        pod_name = clickhouse_pods[0]
        
        # Test default user
        if 'password' in default_user_config:
            password = default_user_config['password']
            result = clickhouse.test_clickhouse_connection(
                namespace=namespace,
                pod_name=pod_name,
                user="default",
                password=password
            )
            assert result, f"Failed to connect with default user (password: {password})"
            note(f"✓ Default user connection successful")
        
        # Test additional users
        for user_config in users_config:
            user_name = user_config.get('name')
            if user_name:
                # For SHA256 hex passwords, we need the plain text password
                # This is a limitation - we'll skip verification for hashed passwords
                # unless we have a password field
                if 'password' in user_config:
                    password = user_config['password']
                    result = clickhouse.test_clickhouse_connection(
                        namespace=namespace,
                        pod_name=pod_name,
                        user=user_name,
                        password=password
                    )
                    assert result, f"Failed to connect with user {user_name}"
                    note(f"✓ User '{user_name}' connection successful")
                else:
                    note(f"⊘ User '{user_name}' has hashed password, skipping connection test")
    
    def verify_keeper(self, namespace):
        """Verify Keeper pods if enabled."""
        keeper_config = self.values.get('keeper', {})
        if keeper_config.get('enabled', False):
            expected_count = keeper_config.get('replicaCount', 0)
            if expected_count > 0:
                clickhouse.verify_keeper_pods_running(
                    namespace=namespace, 
                    expected_count=expected_count
                )
                note(f"✓ Keeper: {expected_count} pods running")
    
    def verify_image(self, namespace):
        """Verify pods are running with correct image."""
        image_config = self.values.get('clickhouse', {}).get('image', {})
        expected_tag = image_config.get('tag')
        
        if expected_tag:
            clickhouse_pods = clickhouse.get_clickhouse_pods(namespace=namespace)
            for pod in clickhouse_pods:
                image = kubernetes.get_pod_image(namespace=namespace, pod_name=pod)
                assert expected_tag in image, \
                    f"Expected image tag {expected_tag}, got {image}"
            note(f"✓ Image tag: {expected_tag}")
    
    def verify_all(self, namespace):
        """Run all verification checks based on configuration."""
        note(f"Verifying deployment state from: {os.path.basename(self.values_file_path)}")
        
        self.verify_deployment(namespace=namespace)
        
        if self.values.get('nameOverride'):
            self.verify_name_override(namespace=namespace)
        
        if self.values.get('clickhouse', {}).get('persistence', {}).get('enabled'):
            self.verify_persistence(namespace=namespace)
        
        if self.values.get('clickhouse', {}).get('lbService', {}).get('enabled'):
            self.verify_service(namespace=namespace)
        
        if self.values.get('clickhouse', {}).get('defaultUser') or \
           self.values.get('clickhouse', {}).get('users'):
            self.verify_users(namespace=namespace)
        
        if self.values.get('keeper', {}).get('enabled'):
            self.verify_keeper(namespace=namespace)
        
        if self.values.get('clickhouse', {}).get('image', {}).get('tag'):
            self.verify_image(namespace=namespace)

"""
Performance Benchmarker
Compare Glue vs Databricks performance to prove migration value

Measures: Latency, Throughput, Cost, Resource Utilization

Author: Analytics360
Version: 2.0.0
"""

from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
import time
import json
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum as spark_sum, avg, current_timestamp

logger = logging.getLogger(__name__)


@dataclass
class BenchmarkMetrics:
    """Performance metrics for a single benchmark run."""
    execution_time_seconds: float
    rows_processed: int
    data_size_mb: float
    throughput_rows_per_sec: float
    throughput_mb_per_sec: float
    cpu_utilization_percent: float
    memory_utilization_percent: float
    shuffle_read_mb: float
    shuffle_write_mb: float
    spill_to_disk_mb: float
    task_count: int
    stage_count: int
    cost_estimate_usd: float


@dataclass
class ComparisonResult:
    """Comparison between Glue and Databricks performance."""
    job_name: str
    glue_metrics: BenchmarkMetrics
    databricks_metrics: BenchmarkMetrics
    speedup_factor: float
    cost_savings_percent: float
    recommendation: str
    detailed_analysis: Dict[str, Any]


class PerformanceBenchmarker:
    """
    Benchmark Glue vs Databricks performance.
    
    Measures:
    - Execution time
    - Throughput (rows/sec, MB/sec)
    - Resource utilization (CPU, memory)
    - Shuffle metrics
    - Cost (DBU vs Glue DPU)
    
    Example:
        ```python
        benchmarker = PerformanceBenchmarker(spark)
        
        # Benchmark a specific query
        result = benchmarker.compare_query_performance(
            query="SELECT * FROM customers WHERE year = 2024",
            glue_execution_time=125.5,  # seconds from Glue logs
            glue_cost=2.50,  # USD
            databricks_cluster_type='i3.xlarge'
        )
        
        print(f"Speedup: {result.speedup_factor}x faster")
        print(f"Cost savings: {result.cost_savings_percent}%")
        print(f"Recommendation: {result.recommendation}")
        ```
    """
    
    # Pricing (approximate, update with actual rates)
    GLUE_DPU_PRICE_PER_HOUR = 0.44  # USD per DPU-hour
    DATABRICKS_DBU_PRICE = 0.15  # USD per DBU (varies by region/edition)
    
    # Instance costs (EC2 pricing + Databricks DBU)
    INSTANCE_COSTS = {
        'i3.xlarge': {'ec2': 0.312, 'dbu_rate': 0.75},  # per hour
        'i3.2xlarge': {'ec2': 0.624, 'dbu_rate': 0.75},
        'm5.2xlarge': {'ec2': 0.384, 'dbu_rate': 0.69},
        'r5.2xlarge': {'ec2': 0.504, 'dbu_rate': 0.69}
    }
    
    def __init__(self, spark: SparkSession):
        """
        Initialize benchmarker.
        
        Args:
            spark: Active SparkSession
        """
        self.spark = spark
        logger.info("PerformanceBenchmarker initialized")
    
    def benchmark_query(
        self,
        query: str,
        iterations: int = 3,
        warmup: bool = True
    ) -> BenchmarkMetrics:
        """
        Benchmark a SQL query on Databricks.
        
        Args:
            query: SQL query to benchmark
            iterations: Number of iterations (median will be used)
            warmup: Run warmup iteration to cache data
        
        Returns:
            BenchmarkMetrics
        """
        logger.info(f"Benchmarking query: {query[:100]}...")
        
        if warmup:
            logger.info("Running warmup iteration...")
            self.spark.sql(query).count()
        
        execution_times = []
        
        for i in range(iterations):
            start = time.time()
            result_df = self.spark.sql(query)
            row_count = result_df.count()
            end = time.time()
            
            execution_time = end - start
            execution_times.append(execution_time)
            logger.info(f"Iteration {i+1}: {execution_time:.2f}s, {row_count} rows")
        
        # Use median execution time
        median_time = sorted(execution_times)[len(execution_times) // 2]
        
        # Get query metrics from Spark UI
        metrics = self._extract_spark_metrics()
        
        # Calculate throughput
        throughput_rows = row_count / median_time if median_time > 0 else 0
        
        # Estimate data size
        data_size_mb = metrics.get('shuffle_read_mb', 0) + metrics.get('shuffle_write_mb', 0)
        throughput_mb = data_size_mb / median_time if median_time > 0 else 0
        
        # Estimate cost
        cost = self._estimate_databricks_cost(median_time, metrics.get('task_count', 1))
        
        return BenchmarkMetrics(
            execution_time_seconds=median_time,
            rows_processed=row_count,
            data_size_mb=data_size_mb,
            throughput_rows_per_sec=throughput_rows,
            throughput_mb_per_sec=throughput_mb,
            cpu_utilization_percent=metrics.get('cpu_utilization', 0),
            memory_utilization_percent=metrics.get('memory_utilization', 0),
            shuffle_read_mb=metrics.get('shuffle_read_mb', 0),
            shuffle_write_mb=metrics.get('shuffle_write_mb', 0),
            spill_to_disk_mb=metrics.get('spill_to_disk_mb', 0),
            task_count=metrics.get('task_count', 0),
            stage_count=metrics.get('stage_count', 0),
            cost_estimate_usd=cost
        )
    
    def compare_query_performance(
        self,
        query: str,
        glue_execution_time: float,
        glue_cost: float,
        glue_dpu_count: int = 10,
        databricks_cluster_type: str = 'i3.xlarge',
        job_name: str = 'benchmark'
    ) -> ComparisonResult:
        """
        Compare Glue vs Databricks performance for a query.
        
        Args:
            query: SQL query to benchmark
            glue_execution_time: Execution time on Glue (seconds)
            glue_cost: Cost on Glue (USD)
            glue_dpu_count: Number of DPUs used on Glue
            databricks_cluster_type: Databricks instance type
            job_name: Name for this benchmark
        
        Returns:
            ComparisonResult
        """
        logger.info(f"Comparing Glue vs Databricks performance for: {job_name}")
        
        # Benchmark on Databricks
        databricks_metrics = self.benchmark_query(query)
        
        # Create Glue metrics from provided data
        glue_metrics = BenchmarkMetrics(
            execution_time_seconds=glue_execution_time,
            rows_processed=0,  # Not available from Glue
            data_size_mb=0,
            throughput_rows_per_sec=0,
            throughput_mb_per_sec=0,
            cpu_utilization_percent=0,
            memory_utilization_percent=0,
            shuffle_read_mb=0,
            shuffle_write_mb=0,
            spill_to_disk_mb=0,
            task_count=0,
            stage_count=0,
            cost_estimate_usd=glue_cost
        )
        
        # Calculate speedup
        speedup = glue_execution_time / databricks_metrics.execution_time_seconds
        
        # Calculate cost savings
        cost_savings = ((glue_cost - databricks_metrics.cost_estimate_usd) / glue_cost) * 100
        
        # Generate recommendation
        recommendation = self._generate_recommendation(speedup, cost_savings)
        
        # Detailed analysis
        detailed_analysis = {
            'execution_time_improvement': f"{speedup:.2f}x faster",
            'time_saved_seconds': glue_execution_time - databricks_metrics.execution_time_seconds,
            'cost_comparison': {
                'glue_usd': glue_cost,
                'databricks_usd': databricks_metrics.cost_estimate_usd,
                'savings_usd': glue_cost - databricks_metrics.cost_estimate_usd
            },
            'throughput_comparison': {
                'databricks_rows_per_sec': databricks_metrics.throughput_rows_per_sec,
                'databricks_mb_per_sec': databricks_metrics.throughput_mb_per_sec
            },
            'resource_efficiency': {
                'shuffle_optimization': databricks_metrics.shuffle_read_mb + databricks_metrics.shuffle_write_mb,
                'spill_to_disk': databricks_metrics.spill_to_disk_mb
            }
        }
        
        logger.info(f"Benchmark complete: {speedup:.2f}x speedup, {cost_savings:.1f}% cost savings")
        
        return ComparisonResult(
            job_name=job_name,
            glue_metrics=glue_metrics,
            databricks_metrics=databricks_metrics,
            speedup_factor=speedup,
            cost_savings_percent=cost_savings,
            recommendation=recommendation,
            detailed_analysis=detailed_analysis
        )
    
    def benchmark_etl_job(
        self,
        source_table: str,
        transformation_func,
        target_table: str,
        job_name: str = 'etl_benchmark'
    ) -> BenchmarkMetrics:
        """
        Benchmark an entire ETL job.
        
        Args:
            source_table: Source table name
            transformation_func: Function that transforms DataFrame
            target_table: Target table name
            job_name: Job name
        
        Returns:
            BenchmarkMetrics
        """
        logger.info(f"Benchmarking ETL job: {job_name}")
        
        start = time.time()
        
        # Read
        source_df = self.spark.table(source_table)
        
        # Transform
        transformed_df = transformation_func(source_df)
        
        # Write
        transformed_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
        
        end = time.time()
        
        execution_time = end - start
        row_count = transformed_df.count()
        
        metrics = self._extract_spark_metrics()
        
        cost = self._estimate_databricks_cost(execution_time, metrics.get('task_count', 1))
        
        return BenchmarkMetrics(
            execution_time_seconds=execution_time,
            rows_processed=row_count,
            data_size_mb=metrics.get('shuffle_read_mb', 0) + metrics.get('shuffle_write_mb', 0),
            throughput_rows_per_sec=row_count / execution_time,
            throughput_mb_per_sec=0,
            cpu_utilization_percent=metrics.get('cpu_utilization', 0),
            memory_utilization_percent=metrics.get('memory_utilization', 0),
            shuffle_read_mb=metrics.get('shuffle_read_mb', 0),
            shuffle_write_mb=metrics.get('shuffle_write_mb', 0),
            spill_to_disk_mb=metrics.get('spill_to_disk_mb', 0),
            task_count=metrics.get('task_count', 0),
            stage_count=metrics.get('stage_count', 0),
            cost_estimate_usd=cost
        )
    
    def _extract_spark_metrics(self) -> Dict[str, Any]:
        """
        Extract metrics from Spark execution.
        
        Note: This is simplified. In production, use Spark's
        query execution listener or Databricks query history API.
        """
        try:
            # Get last query execution
            sc = self.spark.sparkContext
            
            # Simplified metrics - in production, use proper Spark listeners
            metrics = {
                'task_count': 0,
                'stage_count': 0,
                'shuffle_read_mb': 0,
                'shuffle_write_mb': 0,
                'spill_to_disk_mb': 0,
                'cpu_utilization': 0,
                'memory_utilization': 0
            }
            
            # In production, extract from:
            # - spark.sparkContext.statusTracker()
            # - Databricks query history API
            # - Spark UI metrics
            
            return metrics
        
        except Exception as e:
            logger.warning(f"Failed to extract Spark metrics: {e}")
            return {}
    
    def _estimate_databricks_cost(
        self,
        execution_time_seconds: float,
        task_count: int,
        cluster_type: str = 'i3.xlarge',
        worker_count: int = 2
    ) -> float:
        """
        Estimate Databricks cost.
        
        Cost = (EC2 cost + DBU cost) * hours * (1 driver + N workers)
        """
        hours = execution_time_seconds / 3600
        
        instance_info = self.INSTANCE_COSTS.get(cluster_type, {'ec2': 0.312, 'dbu_rate': 0.75})
        
        ec2_cost_per_hour = instance_info['ec2']
        dbu_rate = instance_info['dbu_rate']
        
        # Total cost = (driver + workers) * (EC2 + DBU) * hours
        total_nodes = 1 + worker_count
        cost = total_nodes * (ec2_cost_per_hour + (dbu_rate * self.DATABRICKS_DBU_PRICE)) * hours
        
        return round(cost, 4)
    
    def _generate_recommendation(self, speedup: float, cost_savings: float) -> str:
        """Generate migration recommendation."""
        if speedup > 2.0 and cost_savings > 20:
            return "✅ HIGHLY RECOMMENDED: Significant performance gains and cost savings"
        elif speedup > 1.5 and cost_savings > 10:
            return "✅ RECOMMENDED: Good performance improvement and cost reduction"
        elif speedup > 1.0 and cost_savings > 0:
            return "⚠️ CONSIDER: Moderate performance gains, positive cost impact"
        elif speedup > 1.0:
            return "⚠️ EVALUATE: Performance gains but higher cost"
        else:
            return "❌ NOT RECOMMENDED: Performance regression detected"
    
    def create_benchmark_report(
        self,
        comparisons: List[ComparisonResult],
        catalog: str,
        schema: str,
        report_table: str = 'benchmark_results'
    ):
        """
        Store benchmark results in Delta table.
        
        Args:
            comparisons: List of comparison results
            catalog: Unity Catalog name
            schema: Schema name
            report_table: Table name for results
        """
        full_table = f"{catalog}.{schema}.{report_table}"
        
        # Create table if not exists
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {full_table} (
                job_name STRING,
                benchmark_time TIMESTAMP,
                glue_execution_seconds DOUBLE,
                databricks_execution_seconds DOUBLE,
                speedup_factor DOUBLE,
                glue_cost_usd DOUBLE,
                databricks_cost_usd DOUBLE,
                cost_savings_percent DOUBLE,
                cost_savings_usd DOUBLE,
                recommendation STRING,
                databricks_rows_per_sec DOUBLE,
                databricks_mb_per_sec DOUBLE,
                shuffle_read_mb DOUBLE,
                shuffle_write_mb DOUBLE,
                detailed_analysis STRING
            ) USING DELTA
            TBLPROPERTIES (
                'delta.logRetentionDuration' = 'INTERVAL 90 DAYS',
                'description' = 'Glue vs Databricks performance benchmarks'
            )
        """)
        
        # Insert results
        for comp in comparisons:
            self.spark.sql(f"""
                INSERT INTO {full_table}
                VALUES (
                    '{comp.job_name}',
                    current_timestamp(),
                    {comp.glue_metrics.execution_time_seconds},
                    {comp.databricks_metrics.execution_time_seconds},
                    {comp.speedup_factor},
                    {comp.glue_metrics.cost_estimate_usd},
                    {comp.databricks_metrics.cost_estimate_usd},
                    {comp.cost_savings_percent},
                    {comp.glue_metrics.cost_estimate_usd - comp.databricks_metrics.cost_estimate_usd},
                    '{comp.recommendation}',
                    {comp.databricks_metrics.throughput_rows_per_sec},
                    {comp.databricks_metrics.throughput_mb_per_sec},
                    {comp.databricks_metrics.shuffle_read_mb},
                    {comp.databricks_metrics.shuffle_write_mb},
                    '{json.dumps(comp.detailed_analysis)}'
                )
            """)
        
        logger.info(f"Benchmark results saved to: {full_table}")


class BenchmarkSuite:
    """
    Pre-defined benchmark suite for common migration patterns.
    """
    
    def __init__(self, benchmarker: PerformanceBenchmarker):
        """Initialize with benchmarker instance."""
        self.benchmarker = benchmarker
    
    def run_full_suite(
        self,
        test_data_table: str,
        glue_baseline: Dict[str, Dict[str, float]]
    ) -> List[ComparisonResult]:
        """
        Run full benchmark suite.
        
        Args:
            test_data_table: Table with test data
            glue_baseline: Dict of {test_name: {'time': X, 'cost': Y}}
        
        Returns:
            List of comparison results
        """
        results = []
        
        # Test 1: Simple aggregation
        results.append(self._benchmark_aggregation(test_data_table, glue_baseline.get('aggregation', {})))
        
        # Test 2: Complex join
        results.append(self._benchmark_join(test_data_table, glue_baseline.get('join', {})))
        
        # Test 3: Window functions
        results.append(self._benchmark_window(test_data_table, glue_baseline.get('window', {})))
        
        # Test 4: Write performance
        results.append(self._benchmark_write(test_data_table, glue_baseline.get('write', {})))
        
        return results
    
    def _benchmark_aggregation(self, table: str, glue_baseline: Dict) -> ComparisonResult:
        """Benchmark aggregation query."""
        query = f"""
            SELECT 
                category,
                COUNT(*) as count,
                SUM(amount) as total_amount,
                AVG(amount) as avg_amount
            FROM {table}
            GROUP BY category
        """
        
        return self.benchmarker.compare_query_performance(
            query=query,
            glue_execution_time=glue_baseline.get('time', 60),
            glue_cost=glue_baseline.get('cost', 1.0),
            job_name='aggregation_benchmark'
        )
    
    def _benchmark_join(self, table: str, glue_baseline: Dict) -> ComparisonResult:
        """Benchmark join query."""
        query = f"""
            SELECT 
                t1.id,
                t1.name,
                t2.value
            FROM {table} t1
            JOIN {table} t2 ON t1.id = t2.parent_id
        """
        
        return self.benchmarker.compare_query_performance(
            query=query,
            glue_execution_time=glue_baseline.get('time', 120),
            glue_cost=glue_baseline.get('cost', 2.0),
            job_name='join_benchmark'
        )
    
    def _benchmark_window(self, table: str, glue_baseline: Dict) -> ComparisonResult:
        """Benchmark window function query."""
        query = f"""
            SELECT 
                id,
                category,
                amount,
                ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount DESC) as rank
            FROM {table}
        """
        
        return self.benchmarker.compare_query_performance(
            query=query,
            glue_execution_time=glue_baseline.get('time', 90),
            glue_cost=glue_baseline.get('cost', 1.5),
            job_name='window_benchmark'
        )
    
    def _benchmark_write(self, table: str, glue_baseline: Dict) -> ComparisonResult:
        """Benchmark write performance."""
        query = f"""
            CREATE OR REPLACE TABLE benchmark_output AS
            SELECT * FROM {table}
        """
        
        return self.benchmarker.compare_query_performance(
            query=query,
            glue_execution_time=glue_baseline.get('time', 150),
            glue_cost=glue_baseline.get('cost', 2.5),
            job_name='write_benchmark'
        )


def generate_benchmark_dashboard_sql(catalog: str, schema: str, table: str) -> str:
    """Generate SQL for benchmark dashboard."""
    return f"""
-- Benchmark Dashboard Queries

-- 1. Overall Summary
SELECT 
    COUNT(DISTINCT job_name) as total_jobs_benchmarked,
    AVG(speedup_factor) as avg_speedup,
    AVG(cost_savings_percent) as avg_cost_savings,
    SUM(cost_savings_usd) as total_savings_usd
FROM {catalog}.{schema}.{table};

-- 2. Top Performers
SELECT 
    job_name,
    speedup_factor,
    cost_savings_percent,
    recommendation
FROM {catalog}.{schema}.{table}
ORDER BY speedup_factor DESC
LIMIT 10;

-- 3. Cost Analysis
SELECT 
    job_name,
    glue_cost_usd,
    databricks_cost_usd,
    cost_savings_usd,
    cost_savings_percent
FROM {catalog}.{schema}.{table}
ORDER BY cost_savings_usd DESC;

-- 4. Performance Trends
SELECT 
    DATE(benchmark_time) as date,
    AVG(speedup_factor) as avg_speedup,
    AVG(cost_savings_percent) as avg_savings
FROM {catalog}.{schema}.{table}
GROUP BY DATE(benchmark_time)
ORDER BY date;
"""

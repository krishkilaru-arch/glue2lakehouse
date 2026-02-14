"""
Risk Scoring Models
Credit risk calculations and scoring functions.

Contains UDFs and complex analytics for migration.
"""

from dataclasses import dataclass
from typing import Dict, Any, List, Optional, Tuple
from decimal import Decimal
from enum import Enum
import math
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructType, StructField


class CreditRating(Enum):
    """Credit rating categories."""
    SUPER_PRIME = "A+"      # 750+
    PRIME = "A"             # 700-749
    NEAR_PRIME = "B"        # 650-699
    SUBPRIME = "C"          # 600-649
    DEEP_SUBPRIME = "D"     # Below 600


@dataclass
class RiskScore:
    """Complete risk assessment."""
    application_id: str
    credit_score: int
    credit_rating: CreditRating
    probability_of_default: float
    loss_given_default: float
    expected_loss: float
    risk_tier: int
    approved_rate: Optional[float] = None
    max_approved_amount: Optional[Decimal] = None
    conditions: List[str] = None
    
    @property
    def risk_adjusted_rate(self) -> float:
        """Calculate risk-adjusted interest rate."""
        base_rate = 4.99  # Base rate
        risk_premium = self.probability_of_default * 10  # 10% premium per PD point
        return base_rate + risk_premium
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "application_id": self.application_id,
            "credit_score": self.credit_score,
            "credit_rating": self.credit_rating.value,
            "probability_of_default": self.probability_of_default,
            "loss_given_default": self.loss_given_default,
            "expected_loss": self.expected_loss,
            "risk_tier": self.risk_tier,
            "approved_rate": self.approved_rate,
            "risk_adjusted_rate": self.risk_adjusted_rate
        }


def calculate_risk_score(credit_score: int, dti_ratio: float, 
                         ltv_ratio: float, years_employed: int,
                         delinquencies_24m: int = 0) -> Tuple[float, int]:
    """
    Calculate composite risk score.
    
    This is a complex scoring function that becomes a UDF.
    
    Args:
        credit_score: FICO score
        dti_ratio: Debt to income ratio
        ltv_ratio: Loan to value ratio
        years_employed: Years at current employer
        delinquencies_24m: Delinquencies in last 24 months
    
    Returns:
        Tuple of (risk_score, risk_tier)
    """
    # Credit score component (40% weight)
    if credit_score >= 750:
        credit_component = 100
    elif credit_score >= 700:
        credit_component = 80
    elif credit_score >= 650:
        credit_component = 60
    elif credit_score >= 600:
        credit_component = 40
    else:
        credit_component = 20
    
    # DTI component (25% weight)
    if dti_ratio <= 0.28:
        dti_component = 100
    elif dti_ratio <= 0.36:
        dti_component = 80
    elif dti_ratio <= 0.43:
        dti_component = 60
    elif dti_ratio <= 0.50:
        dti_component = 40
    else:
        dti_component = 20
    
    # LTV component (20% weight)
    if ltv_ratio <= 0.80:
        ltv_component = 100
    elif ltv_ratio <= 0.90:
        ltv_component = 80
    elif ltv_ratio <= 1.00:
        ltv_component = 60
    elif ltv_ratio <= 1.10:
        ltv_component = 40
    else:
        ltv_component = 20
    
    # Employment stability (10% weight)
    if years_employed >= 5:
        emp_component = 100
    elif years_employed >= 3:
        emp_component = 80
    elif years_employed >= 1:
        emp_component = 60
    else:
        emp_component = 40
    
    # Delinquency history (5% weight)
    if delinquencies_24m == 0:
        delinq_component = 100
    elif delinquencies_24m == 1:
        delinq_component = 60
    elif delinquencies_24m == 2:
        delinq_component = 30
    else:
        delinq_component = 0
    
    # Calculate weighted score
    risk_score = (
        credit_component * 0.40 +
        dti_component * 0.25 +
        ltv_component * 0.20 +
        emp_component * 0.10 +
        delinq_component * 0.05
    )
    
    # Determine risk tier
    if risk_score >= 85:
        risk_tier = 1  # Excellent
    elif risk_score >= 70:
        risk_tier = 2  # Good
    elif risk_score >= 55:
        risk_tier = 3  # Fair
    elif risk_score >= 40:
        risk_tier = 4  # Poor
    else:
        risk_tier = 5  # Very Poor
    
    return risk_score, risk_tier


def calculate_probability_of_default(credit_score: int, dti_ratio: float,
                                     loan_amount: float, term_months: int,
                                     loan_type: str) -> float:
    """
    Calculate probability of default using logistic regression model.
    
    This represents a production ML model scoring function.
    
    Args:
        credit_score: FICO score
        dti_ratio: Debt to income ratio
        loan_amount: Loan amount
        term_months: Loan term
        loan_type: Type of loan
    
    Returns:
        Probability of default (0-1)
    """
    # Model coefficients (would come from trained model)
    intercept = 3.5
    coef_credit = -0.008
    coef_dti = 2.5
    coef_amount = 0.00001
    coef_term = 0.02
    
    # Loan type adjustments
    type_adjustments = {
        "AUTO_NEW": -0.3,
        "AUTO_USED": 0.1,
        "PERSONAL": 0.5,
        "HOME_EQUITY": -0.4,
        "REFINANCE": 0.0
    }
    type_adj = type_adjustments.get(loan_type, 0)
    
    # Calculate logit
    logit = (
        intercept +
        coef_credit * credit_score +
        coef_dti * dti_ratio +
        coef_amount * loan_amount +
        coef_term * term_months +
        type_adj
    )
    
    # Apply sigmoid
    probability = 1 / (1 + math.exp(-logit))
    
    # Ensure bounds
    return max(0.001, min(0.999, probability))


def calculate_expected_loss(probability_of_default: float,
                            loan_amount: float,
                            recovery_rate: float = 0.40) -> float:
    """
    Calculate expected loss.
    
    EL = PD × LGD × EAD
    
    Args:
        probability_of_default: PD
        loan_amount: Exposure at default
        recovery_rate: Expected recovery rate
    
    Returns:
        Expected loss amount
    """
    lgd = 1 - recovery_rate  # Loss given default
    return probability_of_default * lgd * loan_amount


def get_credit_rating(credit_score: int) -> CreditRating:
    """Map credit score to rating category."""
    if credit_score >= 750:
        return CreditRating.SUPER_PRIME
    elif credit_score >= 700:
        return CreditRating.PRIME
    elif credit_score >= 650:
        return CreditRating.NEAR_PRIME
    elif credit_score >= 600:
        return CreditRating.SUBPRIME
    else:
        return CreditRating.DEEP_SUBPRIME


def determine_interest_rate(risk_tier: int, term_months: int,
                            loan_type: str) -> float:
    """
    Determine approved interest rate based on risk profile.
    
    Args:
        risk_tier: Risk tier (1-5)
        term_months: Loan term
        loan_type: Loan product type
    
    Returns:
        Interest rate percentage
    """
    # Base rates by tier
    tier_rates = {
        1: 4.99,
        2: 6.99,
        3: 9.99,
        4: 14.99,
        5: 19.99
    }
    
    base_rate = tier_rates.get(risk_tier, 15.99)
    
    # Term adjustment (longer term = higher rate)
    if term_months > 60:
        base_rate += 1.0
    elif term_months > 72:
        base_rate += 2.0
    
    # Type adjustment
    if loan_type == "AUTO_NEW":
        base_rate -= 0.5
    elif loan_type == "PERSONAL":
        base_rate += 2.0
    
    return min(base_rate, 24.99)  # Cap at 24.99%


# ============================================================
# Spark UDF Registration Functions
# ============================================================

def register_risk_udfs(spark):
    """
    Register all risk scoring UDFs with Spark.
    
    Args:
        spark: SparkSession instance
    """
    from pyspark.sql.functions import udf
    
    # Risk score UDF
    @udf(returnType=DoubleType())
    def risk_score_udf(credit_score, dti_ratio, ltv_ratio, years_employed, delinquencies):
        if None in [credit_score, dti_ratio, ltv_ratio, years_employed]:
            return None
        score, _ = calculate_risk_score(
            credit_score, dti_ratio, ltv_ratio, 
            years_employed, delinquencies or 0
        )
        return float(score)
    
    spark.udf.register("calculate_risk_score", risk_score_udf)
    
    # Risk tier UDF
    @udf(returnType=StringType())
    def risk_tier_udf(credit_score, dti_ratio, ltv_ratio, years_employed, delinquencies):
        if None in [credit_score, dti_ratio, ltv_ratio, years_employed]:
            return None
        _, tier = calculate_risk_score(
            credit_score, dti_ratio, ltv_ratio,
            years_employed, delinquencies or 0
        )
        return str(tier)
    
    spark.udf.register("calculate_risk_tier", risk_tier_udf)
    
    # PD UDF
    @udf(returnType=DoubleType())
    def pd_udf(credit_score, dti_ratio, loan_amount, term_months, loan_type):
        if None in [credit_score, dti_ratio, loan_amount, term_months]:
            return None
        return calculate_probability_of_default(
            credit_score, dti_ratio, loan_amount, term_months, loan_type or "AUTO_USED"
        )
    
    spark.udf.register("calculate_pd", pd_udf)
    
    # Credit rating UDF
    @udf(returnType=StringType())
    def credit_rating_udf(credit_score):
        if credit_score is None:
            return None
        return get_credit_rating(credit_score).value
    
    spark.udf.register("get_credit_rating", credit_rating_udf)
    
    # Interest rate UDF
    @udf(returnType=DoubleType())
    def interest_rate_udf(risk_tier, term_months, loan_type):
        if None in [risk_tier, term_months]:
            return None
        return determine_interest_rate(int(risk_tier), term_months, loan_type)
    
    spark.udf.register("determine_interest_rate", interest_rate_udf)


def create_risk_score_df(spark, applications_df):
    """
    Create risk score DataFrame from applications.
    
    This shows complex DataFrame operations that need migration.
    
    Args:
        spark: SparkSession
        applications_df: DataFrame with application data
    
    Returns:
        DataFrame with risk scores
    """
    # Register UDFs
    risk_score_udf = F.udf(
        lambda cs, dti, ltv, ye, del_: calculate_risk_score(cs, dti, ltv, ye, del_ or 0)[0] 
            if None not in [cs, dti, ltv, ye] else None,
        DoubleType()
    )
    
    risk_tier_udf = F.udf(
        lambda cs, dti, ltv, ye, del_: calculate_risk_score(cs, dti, ltv, ye, del_ or 0)[1]
            if None not in [cs, dti, ltv, ye] else None,
        DoubleType()
    )
    
    pd_udf = F.udf(
        lambda cs, dti, amt, term, type_: calculate_probability_of_default(cs, dti, amt, term, type_)
            if None not in [cs, dti, amt, term] else None,
        DoubleType()
    )
    
    # Calculate scores
    scored_df = applications_df.withColumn(
        "risk_score",
        risk_score_udf(
            F.col("credit_score"),
            F.col("dti_ratio"),
            F.col("ltv_ratio"),
            F.col("years_employed"),
            F.col("delinquencies_24m")
        )
    ).withColumn(
        "risk_tier",
        risk_tier_udf(
            F.col("credit_score"),
            F.col("dti_ratio"),
            F.col("ltv_ratio"),
            F.col("years_employed"),
            F.col("delinquencies_24m")
        )
    ).withColumn(
        "probability_of_default",
        pd_udf(
            F.col("credit_score"),
            F.col("dti_ratio"),
            F.col("loan_amount"),
            F.col("term_months"),
            F.col("loan_type")
        )
    )
    
    # Calculate expected loss
    scored_df = scored_df.withColumn(
        "expected_loss",
        F.col("probability_of_default") * 0.60 * F.col("loan_amount")  # 60% LGD
    )
    
    return scored_df

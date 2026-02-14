# ==============================================================================
# MIGRATED FROM AWS GLUE TO DATABRICKS
# Framework: Glue2Lakehouse v4.0 (Production Ready)
# ==============================================================================
#
# Review and test before production use
#
# Transformations:
#   - Removed Job class
#   - Converted DynamicFrame -> DataFrame
#   - Updated type annotations
#   - Syntax: PASSED
#
# ==============================================================================


"""
Unit tests for risk scoring models.
"""

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
import pytest
from decimal import Decimal
from apex_risk_platform.models.risk_models import (
    calculate_risk_score,
    calculate_probability_of_default,
    calculate_expected_loss,
    get_credit_rating,
    determine_interest_rate,
    CreditRating,
    RiskScore
)
from apex_risk_platform.models.loan_models import (
    LoanApplication,
    LoanStatus,
    Applicant,
    Vehicle,
    LoanType,
    calculate_amortization
)
from datetime import date, datetime

class TestRiskScore:
    """Test risk score calculation."""

    def test_excellent_risk_score(self):
        """Test excellent risk profile."""
        score, tier = calculate_risk_score(
            credit_score=780,
            dti_ratio=0.25,
            ltv_ratio=0.75,
            years_employed=10,
            delinquencies_24m=0
        )
        assert score >= 85
        assert tier == 1

    def test_poor_risk_score(self):
        """Test poor risk profile."""
        score, tier = calculate_risk_score(
            credit_score=550,
            dti_ratio=0.50,
            ltv_ratio=1.1,
            years_employed=0,
            delinquencies_24m=3
        )
        assert score < 40
        assert tier >= 4

    def test_mid_tier_risk_score(self):
        """Test mid-tier risk profile."""
        score, tier = calculate_risk_score(
            credit_score=680,
            dti_ratio=0.38,
            ltv_ratio=0.95,
            years_employed=3,
            delinquencies_24m=1
        )
        assert 40 <= score <= 70
        assert tier in [2, 3]

    def test_delinquency_impact(self):
        """Test that delinquencies reduce risk score."""
        score_clean, _ = calculate_risk_score(700, 0.35, 0.85, 5, 0)
        score_delinq, _ = calculate_risk_score(700, 0.35, 0.85, 5, 2)
        assert score_clean > score_delinq

class TestProbabilityOfDefault:
    """Test probability of default calculation."""

    def test_low_pd_for_prime(self):
        """Prime borrower should have low PD."""
        pd = calculate_probability_of_default(
            credit_score=750,
            dti_ratio=0.30,
            loan_amount=25000,
            term_months=48,
            loan_type="AUTO_NEW"
        )
        assert pd < 0.10

    def test_high_pd_for_subprime(self):
        """Subprime borrower should have higher PD."""
        pd = calculate_probability_of_default(
            credit_score=580,
            dti_ratio=0.50,
            loan_amount=25000,
            term_months=72,
            loan_type="AUTO_USED"
        )
        assert pd > 0.15

    def test_pd_increases_with_term(self):
        """Longer terms should increase PD."""
        pd_short = calculate_probability_of_default(700, 0.35, 20000, 36, "AUTO_USED")
        pd_long = calculate_probability_of_default(700, 0.35, 20000, 72, "AUTO_USED")
        assert pd_long > pd_short

    def test_pd_bounds(self):
        """PD should be bounded between 0 and 1."""
        pd = calculate_probability_of_default(850, 0.10, 5000, 12, "AUTO_NEW")
        assert 0 < pd < 1

        pd = calculate_probability_of_default(450, 0.70, 100000, 84, "PERSONAL")
        assert 0 < pd < 1

class TestCreditRating:
    """Test credit rating assignment."""

    def test_super_prime(self):
        assert get_credit_rating(780) == CreditRating.SUPER_PRIME
        assert get_credit_rating(850) == CreditRating.SUPER_PRIME

    def test_prime(self):
        assert get_credit_rating(720) == CreditRating.PRIME
        assert get_credit_rating(749) == CreditRating.PRIME

    def test_near_prime(self):
        assert get_credit_rating(670) == CreditRating.NEAR_PRIME

    def test_subprime(self):
        assert get_credit_rating(620) == CreditRating.SUBPRIME

    def test_deep_subprime(self):
        assert get_credit_rating(550) == CreditRating.DEEP_SUBPRIME
        assert get_credit_rating(400) == CreditRating.DEEP_SUBPRIME

class TestInterestRate:
    """Test interest rate determination."""

    def test_tier1_rate(self):
        """Tier 1 should get lowest rate."""
        rate = determine_interest_rate(1, 48, "AUTO_NEW")
        assert rate < 6.0

    def test_tier5_rate(self):
        """Tier 5 should get highest rate."""
        rate = determine_interest_rate(5, 48, "AUTO_NEW")
        assert rate >= 15.0

    def test_long_term_premium(self):
        """Longer terms should have rate premium."""
        rate_48 = determine_interest_rate(2, 48, "AUTO_USED")
        rate_72 = determine_interest_rate(2, 72, "AUTO_USED")
        assert rate_72 > rate_48

    def test_personal_loan_premium(self):
        """Personal loans should have premium over auto."""
        rate_auto = determine_interest_rate(3, 48, "AUTO_NEW")
        rate_personal = determine_interest_rate(3, 48, "PERSONAL")
        assert rate_personal > rate_auto

class TestLoanAmortization:
    """Test amortization schedule calculation."""

    def test_basic_amortization(self):
        """Test basic amortization calculation."""
        schedule = calculate_amortization(
            principal=Decimal("20000"),
            rate=Decimal("7.99"),
            term_months=48,
            start_date=date(2024, 1, 15)
        )

        assert len(schedule) == 48
        assert schedule[0].payment_number == 1
        assert schedule[-1].payment_number == 48
        assert schedule[-1].remaining_balance <= Decimal("1")  # Should be ~0

    def test_amortization_total_interest(self):
        """Total interest should be reasonable."""
        schedule = calculate_amortization(
            principal=Decimal("25000"),
            rate=Decimal("5.99"),
            term_months=60,
            start_date=date(2024, 1, 1)
        )

        total_interest = sum(p.interest_amount for p in schedule)
        assert total_interest > Decimal("0")
        assert total_interest < Decimal("25000")  # Less than principal

class TestExpectedLoss:
    """Test expected loss calculation."""

    def test_expected_loss_formula(self):
        """EL = PD × LGD × EAD."""
        el = calculate_expected_loss(
            probability_of_default=0.10,
            loan_amount=25000,
            recovery_rate=0.40
        )
        # EL = 0.10 * 0.60 * 25000 = 1500
        assert el == pytest.approx(1500, rel=0.01)

    def test_zero_pd_zero_loss(self):
        """Zero PD should mean zero expected loss."""
        el = calculate_expected_loss(0.0, 25000, 0.40)
        assert el == 0.0

if __name__ == "__main__":
    pytest.main([__file__, "-v"])

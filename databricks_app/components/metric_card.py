"""
Metric Card Component
Enhanced metric display with styling

Author: Analytics360
Version: 2.0.0
"""

import streamlit as st
from typing import Optional, Union


class MetricCard:
    """
    Display enhanced metric cards with formatting and styling.
    
    Example:
        ```python
        MetricCard.render(
            title="Total Cost",
            value=1234.56,
            format="currency",
            delta=-123.45,
            delta_format="currency"
        )
        ```
    """
    
    @staticmethod
    def format_value(value: Union[int, float], format_type: str = "number") -> str:
        """
        Format value based on type.
        
        Args:
            value: Value to format
            format_type: 'number', 'currency', 'percent', 'duration'
        
        Returns:
            Formatted string
        """
        if format_type == "currency":
            return f"${value:,.2f}"
        elif format_type == "percent":
            return f"{value:.1f}%"
        elif format_type == "duration":
            # Assume value is in hours
            if value < 1:
                return f"{value * 60:.0f} min"
            else:
                return f"{value:.1f} hrs"
        elif format_type == "count":
            return f"{int(value):,}"
        else:
            return f"{value:,.2f}" if isinstance(value, float) else f"{value:,}"
    
    @staticmethod
    def render(
        title: str,
        value: Union[int, float],
        format: str = "number",
        delta: Optional[Union[int, float]] = None,
        delta_format: Optional[str] = None,
        icon: Optional[str] = None,
        help_text: Optional[str] = None
    ):
        """
        Render a metric card with formatting.
        
        Args:
            title: Metric title
            value: Main value
            format: Value format type
            delta: Change value (optional)
            delta_format: Delta format type (defaults to same as value)
            icon: Optional icon emoji
            help_text: Optional help text
        """
        # Format main value
        formatted_value = MetricCard.format_value(value, format)
        if icon:
            formatted_value = f"{icon} {formatted_value}"
        
        # Format delta if provided
        formatted_delta = None
        if delta is not None:
            delta_fmt = delta_format or format
            formatted_delta = MetricCard.format_value(abs(delta), delta_fmt)
            # Don't add +/- prefix, Streamlit handles that
        
        st.metric(
            label=title,
            value=formatted_value,
            delta=formatted_delta if delta is not None else None,
            help=help_text
        )
    
    @staticmethod
    def render_grid(metrics: list, columns: int = 4):
        """
        Render multiple metrics in a grid layout.
        
        Args:
            metrics: List of metric configurations
            columns: Number of columns in grid
        
        Example:
            ```python
            MetricCard.render_grid([
                {'title': 'Projects', 'value': 15, 'format': 'count', 'icon': '📊'},
                {'title': 'Cost', 'value': 1234.56, 'format': 'currency', 'delta': -100},
                {'title': 'Progress', 'value': 78.5, 'format': 'percent'},
            ], columns=3)
            ```
        """
        cols = st.columns(columns)
        
        for idx, metric in enumerate(metrics):
            col_idx = idx % columns
            with cols[col_idx]:
                MetricCard.render(
                    title=metric.get('title', ''),
                    value=metric.get('value', 0),
                    format=metric.get('format', 'number'),
                    delta=metric.get('delta'),
                    delta_format=metric.get('delta_format'),
                    icon=metric.get('icon'),
                    help_text=metric.get('help_text')
                )
    
    @staticmethod
    def render_comparison(
        title: str,
        before_value: Union[int, float],
        after_value: Union[int, float],
        format: str = "number",
        before_label: str = "Before",
        after_label: str = "After"
    ):
        """
        Render a before/after comparison.
        
        Args:
            title: Comparison title
            before_value: Value before
            after_value: Value after
            format: Value format type
            before_label: Label for before value
            after_label: Label for after value
        """
        st.write(f"**{title}**")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric(
                label=before_label,
                value=MetricCard.format_value(before_value, format)
            )
        
        with col2:
            delta = after_value - before_value
            st.metric(
                label=after_label,
                value=MetricCard.format_value(after_value, format),
                delta=MetricCard.format_value(delta, format)
            )

"""
Progress Indicator Component
Visual progress bars and indicators

Author: Analytics360
Version: 2.0.0
"""

import streamlit as st
from typing import Optional


class ProgressIndicator:
    """
    Display progress indicators with consistent styling.
    
    Example:
        ```python
        ProgressIndicator.render(
            value=75.5,
            label="Migration Progress",
            show_percentage=True
        )
        ```
    """
    
    @staticmethod
    def render(
        value: float,
        label: Optional[str] = None,
        show_percentage: bool = True,
        color: Optional[str] = None
    ):
        """
        Render a progress bar.
        
        Args:
            value: Progress value (0-100)
            label: Optional label above progress bar
            show_percentage: Show percentage text
            color: Optional color (default based on value)
        """
        # Normalize value to 0-1 range
        normalized_value = min(max(value / 100, 0), 1)
        
        if label:
            st.write(f"**{label}**")
        
        # Show progress bar
        st.progress(normalized_value)
        
        # Show percentage if requested
        if show_percentage:
            # Color code based on progress
            if value >= 90:
                color_emoji = "🟢"
            elif value >= 70:
                color_emoji = "🟡"
            elif value >= 40:
                color_emoji = "🟠"
            else:
                color_emoji = "🔴"
            
            st.caption(f"{color_emoji} {value:.1f}%")
    
    @staticmethod
    def render_segmented(
        segments: list,
        total: float = 100
    ):
        """
        Render a segmented progress bar showing multiple categories.
        
        Args:
            segments: List of dicts with 'label', 'value', 'color'
            total: Total value (default 100)
        
        Example:
            ```python
            ProgressIndicator.render_segmented([
                {'label': 'Completed', 'value': 50, 'color': 'green'},
                {'label': 'In Progress', 'value': 30, 'color': 'orange'},
                {'label': 'Pending', 'value': 20, 'color': 'gray'}
            ])
            ```
        """
        # Calculate percentages
        percentages = [(seg['value'] / total * 100) for seg in segments]
        
        # Create columns for each segment
        cols = st.columns(len(segments))
        
        for col, seg, pct in zip(cols, segments, percentages):
            with col:
                st.metric(
                    label=seg['label'],
                    value=f"{pct:.1f}%",
                    help=f"{seg['value']} / {total}"
                )
    
    @staticmethod
    def render_circular(
        value: float,
        label: str,
        size: str = "medium"
    ):
        """
        Render a circular progress indicator (using Streamlit metrics).
        
        Args:
            value: Progress value (0-100)
            label: Label for the indicator
            size: Size hint ('small', 'medium', 'large')
        """
        # Use metric with progress as the main value
        st.metric(
            label=label,
            value=f"{value:.1f}%",
            delta=f"of 100%"
        )

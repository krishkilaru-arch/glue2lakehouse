"""
Status Card Component
Reusable status display card

Author: Analytics360
Version: 2.0.0
"""

import streamlit as st
from typing import Optional


class StatusCard:
    """
    Display a status card with icon, title, and value.
    
    Example:
        ```python
        StatusCard.render(
            title="Migration Status",
            value="IN PROGRESS",
            icon="🚀",
            color="orange"
        )
        ```
    """
    
    STATUS_COLORS = {
        'REGISTERED': '🔵',
        'IN_PROGRESS': '🟠',
        'COMPLETED': '🟢',
        'FAILED': '🔴',
        'PASSED': '✅',
        'WARNING': '⚠️'
    }
    
    @staticmethod
    def render(
        title: str,
        value: str,
        icon: Optional[str] = None,
        delta: Optional[str] = None,
        help_text: Optional[str] = None
    ):
        """
        Render a status card.
        
        Args:
            title: Card title
            value: Main value to display
            icon: Optional icon (emoji or status key)
            delta: Optional delta/change indicator
            help_text: Optional help text
        """
        # Get icon from status map if value is a known status
        if icon is None and value in StatusCard.STATUS_COLORS:
            icon = StatusCard.STATUS_COLORS[value]
        
        # Display metric with optional icon
        display_value = f"{icon} {value}" if icon else value
        
        if delta:
            st.metric(
                label=title,
                value=display_value,
                delta=delta,
                help=help_text
            )
        else:
            st.metric(
                label=title,
                value=display_value,
                help=help_text
            )
    
    @staticmethod
    def render_multiple(cards: list):
        """
        Render multiple status cards in columns.
        
        Args:
            cards: List of card configurations
                   Each card is a dict with: title, value, icon, delta, help_text
        
        Example:
            ```python
            StatusCard.render_multiple([
                {'title': 'Projects', 'value': '15', 'icon': '📊'},
                {'title': 'Progress', 'value': '78.5%', 'delta': '+5%'},
            ])
            ```
        """
        cols = st.columns(len(cards))
        
        for col, card in zip(cols, cards):
            with col:
                StatusCard.render(
                    title=card.get('title', ''),
                    value=card.get('value', ''),
                    icon=card.get('icon'),
                    delta=card.get('delta'),
                    help_text=card.get('help_text')
                )

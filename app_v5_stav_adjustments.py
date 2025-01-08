import os
from databricks import sql
from databricks.sdk.core import Config
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime,timezone
import time

# Set page configuration for wide layout
st.set_page_config(layout="wide", page_title="Real-Time Dashboard", page_icon="📊")

# Ensure environment variable is set correctly
assert os.getenv('DATABRICKS_WAREHOUSE_ID'), "DATABRICKS_WAREHOUSE_ID must be set in app.yaml."

# CSS for styling
st.markdown(
    """
    <style>
    .block-container {
        padding-top: 2rem; /* Increase top padding */
        padding-bottom: 2rem; /* Increase bottom padding */
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# Main dashboard title
st.markdown("<h1 style='text-align: center;'>Real-Time Dashboard</h1>", unsafe_allow_html=True)

# Cached function to query Databricks
#@st.cache_data(ttl=10)  # Cache results for 10 seconds
def cached_sql_query(query: str) -> pd.DataFrame:
    cfg = Config()
    with sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}",
        credentials_provider=lambda: cfg.authenticate
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall_arrow().to_pandas()

# SQL Queries
query_total_messages = """
SELECT COUNT(*) AS total_messages
FROM data_catalog_dev.default.unique_messages_v2
WHERE DATE(written_date) = CURRENT_DATE()
"""

query_line_chart = """
WITH time_bins AS (
    SELECT
        from_unixtime(UNIX_TIMESTAMP(CURRENT_DATE) + (s.id * 300)) AS time_bin
    FROM (
        SELECT EXPLODE(SEQUENCE(0, 287)) AS id -- 288 intervals of 5 minutes in a day
    ) s
),
ranked_bins AS (
    SELECT
        from_unixtime(floor(unix_timestamp(written_at) / 300) * 300) AS time_bin,
        COUNT(*) AS cnt,
        ROW_NUMBER() OVER (ORDER BY from_unixtime(floor(unix_timestamp(written_at) / 300) * 300) DESC) AS rank
    FROM
        data_catalog_dev.default.unique_messages_v2
    WHERE
        DATE(written_date) = CURRENT_DATE()
    GROUP BY
        from_unixtime(floor(unix_timestamp(written_at) / 300) * 300)
),
filtered_bins AS (
    SELECT
        time_bin,
        cnt,
        CASE WHEN rank = 2 THEN cnt ELSE NULL END AS latest_status
    FROM
        ranked_bins
    WHERE
        rank > 1
)
SELECT
    tb.time_bin,
    COALESCE(fb.cnt, NULL) AS cnt,
    fb.latest_status AS latest_bin
FROM
    time_bins tb
LEFT JOIN
    filtered_bins fb
ON
    tb.time_bin = fb.time_bin
ORDER BY
    tb.time_bin;
"""

query_sentiment_chart = """
SELECT
    sentiment,
    COUNT(*) AS cnt
FROM
    data_catalog_dev.default.unique_messages_v2
WHERE
    DATE(written_date) = CURRENT_DATE() and sentiment IS NOT NULL
GROUP BY
    sentiment
ORDER BY cnt DESC
LIMIT 5
"""

query_sentiment_line_chart = """
WITH AggregatedMessages AS (
  SELECT
    written_date,
    sentiment,
    COUNT(*) AS cnt
  FROM
    data_catalog_dev.default.unique_messages_v2
  WHERE
    DATE(written_date) = CURRENT_DATE() AND sentiment IS NOT NULL
  GROUP BY
    written_date, sentiment
),
SentimentTotals AS (
  SELECT
    sentiment,
    SUM(cnt) AS total_count
  FROM
    AggregatedMessages
  GROUP BY
    sentiment
  ORDER BY
    total_count DESC
  LIMIT 5
),
FilteredMessages AS (
  SELECT
    am.written_date,
    am.sentiment,
    am.cnt
  FROM
    AggregatedMessages am
  JOIN
    SentimentTotals st
  ON
    am.sentiment = st.sentiment
),
RankedMessages AS (
  SELECT
    written_date,
    sentiment,
    cnt,
    RANK() OVER (
      PARTITION BY sentiment
      ORDER BY written_date DESC
    ) AS rank
  FROM
    FilteredMessages
)
SELECT
  written_date,
  sentiment,
  cnt
FROM
  RankedMessages
WHERE
  rank > 1
ORDER BY
  written_date ASC, sentiment
"""


# Create placeholders for dashboard layout
col1, col2 = st.columns(2)  # Row 1
col3, col4 = st.columns(2)  # Row 2

# Create placeholders for each column
col1_placeholder = col1.empty()
col2_placeholder = col2.empty()
col3_placeholder = col3.empty()
col4_placeholder = col4.empty()

def update_dashboard(counter):
    # Fetch data and update each chart/section
    data_total = cached_sql_query(query_total_messages)
    total_messages = data_total['total_messages'].iloc[0] if not data_total.empty else 0

    data_line_chart = cached_sql_query(query_line_chart)
    data_line_chart['time_bin'] = pd.to_datetime(data_line_chart['time_bin'])
    data_line_chart['time_bin_formatted'] = data_line_chart['time_bin'].dt.strftime("%Y-%m-%d %H:%M")

    data_sentiment_chart = cached_sql_query(query_sentiment_chart).sort_values(by="cnt", ascending=False)

    data_sentiment_line_chart = cached_sql_query(query_sentiment_line_chart)
    data_sentiment_line_chart['written_date'] = pd.to_datetime(data_sentiment_line_chart['written_date'])

    # Update Row 1
    with col1_placeholder.container():
        st.markdown("<h2 style='text-align: center;'>Comments Over Time</h2>", unsafe_allow_html=True)
        fig = px.line(
            data_line_chart,
            x="time_bin_formatted",
            y=["cnt"],
            labels={"value": "Count", "time_bin_formatted": "Time"},
            markers=True
        )

        # Add vertical lines for the "latest_bin" times
        for _, row in data_line_chart.iterrows():
            if not pd.isna(row["latest_bin"]):  # Check if latest_bin is not NaN
                fig.add_shape(
                    type="line",
                    x0=row["time_bin_formatted"],
                    y0=0,
                    x1=row["time_bin_formatted"],
                    y1=data_line_chart["cnt"].max(),  # Max value for the vertical line
                    line=dict(color="red", width=2, dash="dash")  # Dashed red line
                )

        # Add dummy trace for legend
        fig.add_trace(
            dict(
                x=[None],  # No actual points, just a legend entry
                y=[None],
                mode="lines",
                line=dict(color="red", width=2, dash="dash"),
                name="Latest Bin (Dashed Line)"  # Legend entry name
            )
        )

        fig.update_layout(margin=dict(t=20, b=50, l=40, r=40))
        st.plotly_chart(fig, use_container_width=True, key=f"line_chart_{counter}")

    with col2_placeholder.container():
        st.markdown("<h2 style='text-align: center;'>Live: Today's Comments</h2>", unsafe_allow_html=True)
        st.markdown(
            f"<h1 style='text-align: center; font-size: 3rem;'>{total_messages:,}</h1>",  # Increase font size
            unsafe_allow_html=True,
        )
        # Add the current update time in UTC
        current_time_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        st.markdown(
            f"<p style='text-align: center; font-size: 1rem; color: gray;'>Last updated: {current_time_utc}</p>",
            unsafe_allow_html=True,
        )

    # Update Row 2
    with col3_placeholder.container():
        st.markdown("<h2 style='text-align: center;'>Sentiment Distribution</h2>", unsafe_allow_html=True)
        fig_bar = px.bar(
            data_sentiment_chart,
            x="sentiment",
            y="cnt",
            color="sentiment",
            labels={"sentiment": "Sentiment", "cnt": "Count"},
            text="cnt"
        )
        fig_bar.update_traces(textposition='outside',texttemplate='%{text:,}')
        st.plotly_chart(fig_bar, use_container_width=True, key=f"bar_chart_{counter}")

    with col4_placeholder.container():
        # Title
        st.markdown("<h2 style='text-align: center;'>Progress Toward Goal</h2>", unsafe_allow_html=True)

        # Calculate progress
        total_messages_so_far = total_messages  # Total messages fetched earlier
        goal_messages = 1_100_000
        remaining_messages = max(goal_messages - total_messages_so_far, 0)

        # Create the main dataframe for percentages
        progress_data = pd.DataFrame({
            "Category": ["Messages So Far", "Remaining"],
            "Count": [total_messages_so_far, remaining_messages]
        })
        progress_data["Percentage"] = progress_data["Count"] / goal_messages * 100  # Convert to percentage

        # Create the stacked bar chart
        fig_stacked_bar = px.bar(
            progress_data,
            x=["Goal Progress", "Goal Progress"],  # Single x-axis category
            y="Percentage",  # Use percentage values
            color="Category",
            labels={"Percentage": "Percentage (%)", "Category": "Progress"},
            text="Percentage"
        )

        # Customize the appearance of the bars
        fig_stacked_bar.update_traces(
            texttemplate='%{text:.2f}%',  # Format percentages with 2 decimal places
            textposition="inside"
        )

        # Make the "Remaining" section fully transparent and remove its text
        fig_stacked_bar.update_traces(
            selector=dict(name="Remaining"),
            marker=dict(opacity=0),  # Make it fully transparent
            text="",  # Remove any text for "Remaining"
            texttemplate=None,  # Ensure no text template for "Remaining"
            showlegend=False,  # Optionally hide "Remaining" from the legend
        )

        # Add a fixed "100%" label at the top
        fig_stacked_bar.add_annotation(
            x="Goal Progress",  # Centered at the single bar
            y=100,  # Positioned at the top of the y-axis
            text="100%",  # The fixed label
            showarrow=False,  # No arrow
            font=dict(size=14, color="white"),  # Customize font size and color
            align="center",
            xanchor="center",
            yanchor="bottom"
        )

        # Adjust layout to fix alignment issues
        fig_stacked_bar.update_layout(
            yaxis=dict(
                range=[0, 100],  # Keep the y-axis range consistent
                title=None,  # Remove y-axis title to reduce padding
                ticklabelposition="outside",  # Ensure tick labels are outside the axis
                tickfont=dict(size=12),  # Ensure tick labels have consistent size
                automargin=True  # Enable automatic margin adjustments
            ),
            xaxis=dict(
                title="",
                showticklabels=False,  # Hide x-axis tick labels
            ),
            margin=dict(t=50, b=0, l=50, r=20),  # Remove top and bottom padding
            height=400,  # Match height with Chart 3
        )

        # Render the chart
        st.plotly_chart(fig_stacked_bar, use_container_width=False, key=f"stacked_bar_{counter}")


counter = 0
while True:
    update_dashboard(counter)
    counter += 1
    time.sleep(10)

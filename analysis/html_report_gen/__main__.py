#!/usr/bin/env python3

import argparse
import json
from pathlib import Path
from typing import Dict, List, Any, Optional
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.io as pio
from jinja2 import Template, FileSystemLoader, Environment

from analysis.result import Result


class ReportGenerator:
    def __init__(self, config_name: str, result: Result):
        self.config_name = config_name
        self.result = result
        self.template = self._load_template()

    def _load_template(self) -> Template:
        template_path = Path(__file__).parent / "template" / "index.html"

        if template_path.exists():
            # Load template from file
            env = Environment(loader=FileSystemLoader(template_path.parent))
            return env.get_template(template_path.name)
        else:
            raise ValueError(f"Could not find template file {template_path}")

    def generate_report(self, output_path: str) -> None:
        template_vars = {
            'config_name': self.config_name,
            'config_details': self._generate_config_section(),
            'slo_section': self._generate_slo_section(),
            'load_section': self._generate_load_section(),
            'cluster_utilization_section': self._generate_cluster_utilization_section(),
            'scheduler_runtime_section': self._generate_scheduler_runtime_section(),
            'solver_stats_section': self._generate_solver_stats_section(),
            'strl_section': self._generate_strl_section()
        }

        html_content = self.template.render(**template_vars)

        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)


    def _generate_config_section(self) -> str:
        with open(self.result.conf_file, 'r') as f:
            data = f.read()
        return data

    def _generate_slo_section(self) -> str:
        slo_percent = self._calculate_slo_percent()
        task_counts = self._get_task_counts()

        html = f"""
        <div class="metrics">
            <div class="metric-card">
                <div class="metric-value">{slo_percent:.2f}%</div>
                <div class="metric-label">SLO</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{task_counts['total']}</div>
                <div class="metric-label">Total task graphs</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{task_counts['finished']}</div>
                <div class="metric-label">Finished task graphs</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{task_counts['cancelled']}</div>
                <div class="metric-label">Cancelled task graphs</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{task_counts['missed_deadline']}</div>
                <div class="metric-label">Missed deadlines</div>
            </div>
        </div>
        """
        return html

    def _calculate_slo_percent(self) -> float:
        """Calculate SLO achievement percentage."""
        return self.result.slo

    def _get_task_counts(self) -> Dict[str, int]:
        """Get task count statistics."""
        parts = self.result.last_log_stats_line()
        assert parts is not None

        finished = int(parts[5])
        cancelled = int(parts[6])
        missed = int(parts[7])

        return {
            'total': finished + cancelled,
            'finished': finished,
            'cancelled': cancelled,
            'missed_deadline': missed,
        }

    def _generate_load_section(self) -> str:
        """Generate load analysis charts using Plotly."""
        # Generate stacked bar chart for task difficulty partition
        stacked_bar_chart = self._task_graph_difficulty_distribution()

        # Generate load over time chart
        load_over_time_chart = self.scheduler_load_over_time()

        [easy, medium, hard], total = self.result.arrival_rate

        html = f"""
        <h3>Arrival rates</h3>
        <div>
            <table border="1" cellpadding="5" cellspacing="0" style="width: 100%; table-layout: fixed;">
                <thead>
                    <tr>
                        <th style="width: 25%;">Easy</th>
                        <th style="width: 25%;">Medium</th>
                        <th style="width: 25%;">Hard</th>
                        <th style="width: 25%;">Total</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td style="text-align: center;">{easy}</td>
                        <td style="text-align: center;">{medium}</td>
                        <td style="text-align: center;">{hard}</td>
                        <td style="text-align: center; font-weight: bold; background-color: #f0f0f0;">{total}</td>
                    </tr>
                </tbody>
            </table>
        </div>
        <h3>Distribution</h3>
        <div class="chart-container">
            <div id="task-graph-difficulty-distribution" class="plotly-chart"></div>
            {stacked_bar_chart}
        </div>
        <div class="chart-container">
            <div id="scheduler-load-over-time" class="plotly-chart"></div>
            {load_over_time_chart}
        </div>
        """
        return html

    def _task_graph_difficulty_distribution(self) -> str:
        """Create Plotly stacked bar chart for task difficulty distribution."""
        periods = ['']

        num_invocations = self.result.num_invocations

        fig = go.Figure()
        if len(num_invocations) == 1:
            fig.add_trace(go.Bar(name='All', x=periods, y=[num_invocations[1]]))
        else:
            [easy,med,hard] = num_invocations
            fig.add_trace(go.Bar(name='Easy', x=periods, y=[easy]))
            fig.add_trace(go.Bar(name='Medium', x=periods, y=[med]))
            fig.add_trace(go.Bar(name='Hard', x=periods, y=[hard]))

        fig.update_layout(
            title='Task Graph Difficulty Distribution',
            barmode='stack',
            yaxis_title='Number of Tasks'
        )

        return fig.to_html(include_plotlyjs=False, div_id="task-graph-difficulty-distribution")

    def scheduler_load_over_time(self) -> str:
        """Create Plotly line chart for load distribution over time."""

        sched_start_events = [parts for parts in self.result.events if parts[1] == 'SCHEDULER_START']
        sched_finished_events = [parts for parts in self.result.events if parts[1] == 'SCHEDULER_FINISHED']

        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=[int(parts[0]) for parts in sched_start_events],
                y=[int(parts[2]) for parts in sched_start_events],
                mode='lines',
                name='Schedulable tasks',
            )
        )
        fig.add_trace(
            go.Scatter(
                x=[int(parts[0]) for parts in sched_start_events],
                y=[int(parts[3]) for parts in sched_start_events],
                mode='lines',
                name='Currently placed tasks',
            )
        )

        fig.update_layout(
            title='Scheduler load over time',
            xaxis_title='Time',
            yaxis_title='Number of Tasks',
        )

        return fig.to_html(include_plotlyjs=False, div_id="scheduler-load-over-time")
    
    def _generate_cluster_utilization_section(self) -> str:
        """Generate cluster utilization with summary and individual resource sections."""
        summary_metrics = self._create_cluster_summary_metrics()
        individual_resources = self._create_individual_resource_sections()
        
        html = f"""
        <h3>Summary</h3>
        {summary_metrics}
        
        <h3>Individual Resource Utilization</h3>
        {individual_resources}
        """
        return html
    
    def _create_cluster_summary_metrics(self) -> str:
        """Create overall cluster utilization summary metrics."""
        utilization = self.result.cluster_utilization
        
        # Calculate overall averages across all resources
        total_resources = len(utilization)
        avg_effective = sum(util['eff'] for util in utilization.values()) / total_resources
        avg_total = sum(util['tot'] for util in utilization.values()) / total_resources
        
        # Find best and worst performing resources
        best_resource = max(utilization.items(), key=lambda x: x[1]['eff'])
        worst_resource = min(utilization.items(), key=lambda x: x[1]['eff'])
        
        summary_html = f"""
        <div class="metrics">
            <div class="metric-card">
                <div class="metric-value">{len(utilization)}</div>
                <div class="metric-label">Resources</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{avg_effective:.1%}</div>
                <div class="metric-label">Effective Utilization (avg)</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{avg_total:.1%}</div>
                <div class="metric-label">Total Utilization (avg)</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{best_resource[0].upper()}</div>
                <div class="metric-label">Best Resource ({best_resource[1]['eff']:.1%})</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{worst_resource[0].upper()}</div>
                <div class="metric-label">Worst Resource ({worst_resource[1]['eff']:.1%})</div>
            </div>
        </div>
        """
        return summary_html
    
    def _create_individual_resource_sections(self) -> str:
        """Create individual sections for each resource with plot + metrics."""
        utilization = self.result.cluster_utilization
        
        sections_html = ""
        
        for resource, util in utilization.items():
            # Create individual area chart for this resource
            chart_html = self._create_single_resource_chart(resource, util)
            
            # Create metrics for this resource
            eff_percent = util['eff'] * 100
            tot_percent = util['tot'] * 100
            
            metrics_html = f"""
            <div class="metrics">
                <div class="metric-card">
                    <div class="metric-value">{eff_percent:.1f}%</div>
                    <div class="metric-label">Effective (avg)</div>
                </div>
                <div class="metric-card">
                    <div class="metric-value">{tot_percent:.1f}%</div>
                    <div class="metric-label">Total (avg)</div>
                </div>
            </div>
            """
            
            sections_html += f"""
            <div class="resource-section">
                <h4>{resource.upper()} Utilization</h4>
                {metrics_html}
                {chart_html}
            </div>
            """
        
        return sections_html
    
    def _create_single_resource_chart(self, resource: str, util: dict) -> str:
        """Create area chart for a single resource."""
        usage_map = util['series']
        time_points = list(range(len(usage_map)))
        
        # Extract good and bad utilization values
        good_utilization = [good for good, bad in usage_map]
        bad_utilization = [bad for good, bad in usage_map]
        total_utilization = [good + bad for good, bad in zip(good_utilization, bad_utilization)]
        
        # Create single area chart
        fig = go.Figure()
        
        # Add good utilization area (bottom layer)
        fig.add_trace(go.Scatter(
            x=time_points,
            y=good_utilization,
            fill='tozeroy',
            mode='lines',
            name='Good Utilization',
            fillcolor='rgba(46, 204, 113, 0.6)',
            line=dict(color='#2ecc71', width=1),
            hovertemplate=f'{resource.upper()} Good: %{{y:.1%}}<extra></extra>'
        ))
        
        # Add total utilization area (includes good + bad)
        fig.add_trace(go.Scatter(
            x=time_points,
            y=total_utilization,
            fill='tonexty',
            mode='lines',
            name='Total Utilization',
            fillcolor='rgba(231, 76, 60, 0.6)',
            line=dict(color='#e74c3c', width=1),
            hovertemplate=f'{resource.upper()} Total: %{{y:.1%}}<extra></extra>'
        ))
        
        fig.update_layout(
            title=f'{resource.upper()} Utilization Over Time',
            xaxis_title='Time',
            yaxis_title='Utilization',
            height=400,
            showlegend=True,
            hovermode='x unified'
        )
        
        chart_id = f"{resource}-utilization-chart"
        return fig.to_html(include_plotlyjs=False, div_id=chart_id)

    def _generate_scheduler_runtime_section(self) -> str:
        """Generate scheduler runtime analysis using Plotly."""
        # Generate box plot for runtime distribution
        box_plot_chart = self._create_scheduler_runtime_boxplot()

        # Generate interactive runtime over time chart
        runtime_over_time_chart = self._create_scheduler_runtime_over_time_plot()

        html = f"""
        <div class="chart-container">
            <div id="scheduler-runtime-boxplot" class="plotly-chart"></div>
            {box_plot_chart}
        </div>
        <div class="chart-container">
            <div id="scheduler-runtime-over-time-plot" class="plotly-chart"></div>
            {runtime_over_time_chart}
        </div>
        """
        return html

    def _create_scheduler_runtime_boxplot(self) -> str:
        """Create Plotly box plot for scheduler runtime distribution."""

        runtime_data = self.result.scheduler_runtimes

        fig = go.Figure()
        fig.add_trace(go.Box(
            y=runtime_data,
            name='Scheduler Runtime',
            boxpoints='outliers',
            jitter=0.3,
            pointpos=-1.8
        ))
        fig.update_layout(
            title='Scheduler Runtime Distribution',
            yaxis_title='Runtime (s)',
            showlegend=False
        )

        return fig.to_html(include_plotlyjs=False, div_id="scheduler-runtime-boxplot")
        

    def _create_scheduler_runtime_over_time_plot(self) -> str:
        """Create Plotly line chart for scheduler runtime over time."""

        runtime_data = self.result.scheduler_runtimes

        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=list(range(0, len(runtime_data))),
                y=runtime_data,
                mode='lines',
            )
        )
        fig.update_layout(
            title='Scheduler Runtime across invocations',
            yaxis_title='Runtime (s)',
            showlegend=False
        )

        return fig.to_html(include_plotlyjs=False, div_id="scheduler-runtime-over-time-plot")
        
    def _generate_solver_stats_section(self) -> str:
        """Generate TetriSched solver statistics using Plotly."""
        # Generate box plot for solver runtime
        solver_box_plot = self._create_solver_runtime_boxplot()

        # Generate solver runtime over time
        solver_runtime_chart = self._create_solver_runtime_over_time_plot()

        html = f"""
        <div class="chart-container">
            <h4>Solver Runtime Distribution</h4>
            <div id="solver-runtime-boxplot" class="plotly-chart"></div>
            {solver_box_plot}
        </div>
        <div class="chart-container">
            <h4>Solver Runtime Over Time</h4>
            <div id="solver-runtime-over-time-plot" class="plotly-chart"></div>
            {solver_runtime_chart}
        </div>
        """
        return html

    def _create_solver_runtime_boxplot(self) -> str:
        """Create Plotly box plot for solver runtime."""
        runtime_data = self.result.solver_times

        fig = go.Figure()
        fig.add_trace(go.Box(
            y=runtime_data,
            name='Solver Runtime',
            boxpoints='outliers',
            jitter=0.3,
            pointpos=-1.8
        ))
        fig.update_layout(
            title='Solver Runtime Distribution',
            yaxis_title='Runtime (s)',
            showlegend=False
        )

        return fig.to_html(include_plotlyjs=False, div_id="solver-runtime-boxplot")


    def _create_solver_runtime_over_time_plot(self) -> str:
        """Create Plotly chart for solver runtime over time."""
        runtime_data = self.result.solver_times

        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=list(range(0, len(runtime_data))),
                y=runtime_data,
                mode='lines',
            )
        )
        fig.update_layout(
            title='Solver Runtime across invocations',
            yaxis_title='Runtime (s)',
            showlegend=False
        )

        return fig.to_html(include_plotlyjs=False, div_id="solver-runtime-over-time-plot")


    def _generate_strl_section(self) -> str:
        """Generate STRL compiler performance flamegraph using Plotly."""
        flamegraph_chart = self._create_plotly_flamegraph()

        html = f"""
        <div class="chart-container">
            <h4>Compiler Performance Flamegraph</h4>
            <div id="flamegraphChart" class="plotly-chart"></div>
            {flamegraph_chart}
        </div>
        """
        return html

    def _create_plotly_flamegraph(self) -> str:
        """Create flamegraph-style visualization using Plotly."""
        return """TODO"""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("results_dir", type=Path, help="Path to directory containing results")
    parser.add_argument("--output-path", default="report.html", type=Path, help="Path to dump HTML report")

    args = parser.parse_args()

    # Generate report
    result = Result(args.results_dir)
    generator = ReportGenerator("report", result)
    generator.generate_report(args.output_path)
    print(f"Report generated: {args.output_path}")


if __name__ == "__main__":
    main()

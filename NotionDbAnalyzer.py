import matplotlib.pyplot as plt
import plotly.figure_factory as ff
import plotly.graph_objects as go
import seaborn as sns
import pandas as pd

class ActivityAnalyzer:
    def __init__(self,data_filepath, export_path = 'analysis', plotly=True):
        # Load and clean your data (replace with the correct path to your CSV file)
        self.activity_data = pd.read_csv(data_filepath)
        self.activity_data['created_time'] = pd.to_datetime(self.activity_data['created_time'])
        self.activity_data['day_of_week'] = self.activity_data['created_time'].dt.day_name()
        self.export_path = export_path
        self.plotly = plotly
        
        
    def plot_heatmap(self):
        # Calculate the average completion rate per day for each activity
        activity_completion_rate = self.activity_data.groupby(['day_of_week', 'activity'])['properties.Yes.checkbox'].mean().reset_index()
        heatmap_data = activity_completion_rate.pivot_table(index='activity', columns='day_of_week', values='properties.Yes.checkbox')
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        heatmap_data = heatmap_data[day_order]
        # Fill NaN values with a default value (0 in this case)
        heatmap_data = heatmap_data.fillna(0)

        if self.plotly:
            # Calculate the average completion rate per day for each activity
            activity_completion_rate = self.activity_data.groupby(['day_of_week', 'activity'])['properties.Yes.checkbox'].mean().reset_index()
            heatmap_data = activity_completion_rate.pivot_table(index='activity', columns='day_of_week', values='properties.Yes.checkbox')
            day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            heatmap_data = heatmap_data[day_order]

            # Create the heatmap
            fig = ff.create_annotated_heatmap(z=heatmap_data.values, x=day_order, y=heatmap_data.index.to_list(), annotation_text=heatmap_data.values.round(2), colorscale='YlGnBu')
            fig.update_layout(title='Average Completion Rate for Each Activity Across Days of the Week', xaxis_title='Day of the Week', yaxis_title='Activity')
            fig.show()
        else:
            # Create the heatmap with increased figure size and smaller annotation font size
            plt.figure(figsize=(16, 9))
            sns.heatmap(heatmap_data, annot=True, cmap='YlGnBu', fmt=".2f", annot_kws={"size": 10})
            plt.title('Average Completion Rate for Each Activity Across Days of the Week', fontsize=16)
            plt.xlabel('Day of the Week', fontsize=12)
            plt.ylabel('Activity', fontsize=12)

            plt.savefig(self.export_path + '/heatmapAnalysis.png')
        
    def plot_overall_trend(self):
        # Resample the data to get daily completed activities and calculate the 7-day rolling average
        daily_completed_activities = self.activity_data[self.activity_data['properties.Yes.checkbox']].resample('D', on='created_time').size()
        rolling_avg_week = daily_completed_activities.rolling(window=7).mean()

        if self.plotly:
            # Create the plot
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=daily_completed_activities.index, y=daily_completed_activities.values, mode='lines+markers', name='Daily Completed Activities'))
            fig.add_trace(go.Scatter(x=rolling_avg_week.index, y=rolling_avg_week.values, mode='lines', name='7-Day Rolling Average'))
            fig.update_layout(title='Number of Completed Activities Per Day with 7-Day Rolling Average', xaxis_title='Date', yaxis_title='Number of Completed Activities')
            fig.show()
        else:
            # Create the plot
            plt.figure(figsize=(12, 6))
            daily_completed_activities.plot(kind='line', color='b', marker='o', label='Daily Completed Activities')
            rolling_avg_week.plot(kind='line', color='r', linewidth=2, label='7-Day Rolling Average')
            plt.title('Number of Completed Activities Per Day with 7-Day Rolling Average')
            plt.xlabel('Date')
            plt.ylabel('Number of Completed Activities')
            plt.legend()
            plt.grid(True)
            plt.savefig(self.export_path + '/overalltrend.png')
        
    def plot_weekly_completetion_rate_by_activity(self):
        activity_data = self.activity_data
        if self.plotly:
            # Create a time series plot for the weekly completion rate of all activities
            fig = go.Figure()
            unique_activities = activity_data['activity'].unique()

            # Loop through each unique activity to plot its weekly completion rate
            for activity in unique_activities:
                activity_data_filtered = activity_data[activity_data['activity'] == activity]
                weekly_completion_rate = activity_data_filtered.resample('W-MON', on='created_time')['properties.Yes.checkbox'].mean()
                fig.add_trace(go.Scatter(x=weekly_completion_rate.index, y=weekly_completion_rate.values, mode='lines+markers', name=activity))

            fig.update_layout(title='Weekly Completion Rate for All Activities', xaxis_title='Week', yaxis_title='Completion Rate')
            fig.show()
        else:
            # Create a time series plot for the weekly completion rate of all activities
            plt.figure(figsize=(12, 6))
            unique_activities = activity_data['activity'].unique()

            # Loop through each unique activity to plot its weekly completion rate
            for activity in unique_activities:
                activity_data_filtered = activity_data[activity_data['activity'] == activity]
                weekly_completion_rate = activity_data_filtered.resample('W-MON', on='created_time')['properties.Yes.checkbox'].mean()
                weekly_completion_rate.plot(kind='line', marker='o', linewidth=2, label=activity)

            plt.title('Weekly Completion Rate for All Activities')
            plt.xlabel('Week')
            plt.ylabel('Completion Rate')
            plt.legend()
            plt.grid(True)
            plt.savefig(self.export_path + '/weeklycompletion.png')




if __name__ == '__main__':
    data_filepath = 'all_activity.csv'
    analyzer = ActivityAnalyzer(data_filepath)
    analyzer.plot_heatmap()
    analyzer.plot_overall_trend()
    analyzer.plot_weekly_completetion_rate_by_activity()
    
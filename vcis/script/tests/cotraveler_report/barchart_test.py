def get_barchart(self,date_counts_main, date_counts_cotraveler):
        date_counts_cotraveler = pd.concat([date_counts_main, date_counts_cotraveler])
        
        days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        date_counts_cotraveler['all_dates'] = pd.Categorical(date_counts_cotraveler['all_dates'], categories=days_order, ordered=True)

        fig, ax = plt.subplots(figsize=(10, 6))
        
        palette = ["#fee090","#fdae61","#4575b4","#313695","#e0f3f8","#abd9e9","#d73027", "#a50026"]
        
        main_device_id = date_counts_main['device_id'].iloc[0]
        main_device_data = date_counts_cotraveler[date_counts_cotraveler['device_id'] == main_device_id]
        sns.barplot(x='all_dates', y='count', data=main_device_data, ax=ax, color=palette[0], label=main_device_id, ci=None)
        
        other_devices_data = date_counts_cotraveler[date_counts_cotraveler['device_id'] != main_device_id]
        num_other_devices = len(other_devices_data['device_id'].unique())
        for idx, (device_id, group) in enumerate(other_devices_data.groupby('device_id')):
            device_color = palette[idx + 1] 
            sns.barplot(x='all_dates', y='count', data=group, ax=ax, color=device_color, label=device_id, ci=None)

            for bar in ax.patches[-len(group):]:
                bar.set_width(bar.get_width() / num_other_devices+0.1)
                bar.set_x(bar.get_x() + idx * bar.get_width() / num_other_devices)
        
        ax.set_title('Distribution of Hits per Day of the Week')
        ax.set_xlabel('Day of the Week')
        ax.set_ylabel('Count')
        
        handles, labels = ax.get_legend_handles_labels()
        by_label = dict(zip(labels, handles))
        ax.legend(by_label.values(), by_label.keys(), title='Device ID')
        fig.savefig('plot.png')
        return fig
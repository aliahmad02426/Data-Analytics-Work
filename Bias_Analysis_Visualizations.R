
# Benchmarking Graph
graph_benchmarking <- function(benchmarked_data, start_date, end_date, input) {

  plot <- benchmarked_data %>%
    filter(Call_time >= ymd(start_date), Call_time < ymd(end_date)) %>%
    mutate(date_ = as.Date(Call_time),
           time_ = format(ymd_hms(Call_time), "%H:%M:%S"),
           time_ = as.POSIXct(strptime(time_, format="%H:%M:%S")),
           on_off = as.factor(on_off)) %>%
    ggplot(aes(x = time_, y = date_, color = on_off)) +
    geom_point(size = 0.1) +
    theme_bw() +
    theme(panel.background = element_blank(),
          plot.background = element_blank(),
          legend.position = "top",
          plot.title = element_text(hjust = 0.5, size = 25, face = "bold")
    ) +
    scale_x_datetime(name = "Time of Day", breaks = date_breaks("2 hours"), labels = date_format("%H:%M")) +
    scale_y_date(name = "Date", date_breaks = "1 day", date_labels = "%d-%b-%Y") +
    scale_color_discrete(name = "Benchmark", labels = c("OFF", "ON")) +
    guides(color = guide_legend(override.aes = list(size = 5), reverse = TRUE)) +
    ggtitle(paste0(input$percent_on*100, "-", (1-input$percent_on)*100, "_",
                   input$cycle_time, "min",
                   benchmarking_key[[input$cycle_type]], "Benchmarking"))


  return(plot)
}

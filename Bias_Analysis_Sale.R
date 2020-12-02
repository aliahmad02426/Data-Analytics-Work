
#aggregate for the day/skill

bias_analysis_sale_per_day_per_skill <- function(benchmarked_data, percetOn) {
  benchmarked_data <- benchmarked_data %>%
    group_by(Call_Year, Call_Month, Call_Week, Call_Date, Skill) %>%
    summarise(
      On_Calls = n_distinct(Call_Id[on_off == 1]),
      Off_Calls = n_distinct(Call_Id[on_off == 0]),
      Total_Calls = n_distinct(Call_Id),
      On_Sales = sum(Sale[on_off == 1]),
      Off_Sales = sum(Sale[on_off == 0]),
      Total_Sales = sum(Sale),
      On_CR = sum(Sale[on_off == 1]) / n_distinct(Call_Id[on_off == 1]),
      Off_CR = sum(Sale[on_off == 0]) / n_distinct(Call_Id[on_off == 0]),
      On_STD = sd(Sale[on_off == 1]),
      Off_STD = sd(Sale[on_off == 0])
    ) %>%
    mutate(Inc_Sales = (On_Sales / On_Calls -
                          Off_Sales / Off_Calls) * On_Calls)
  benchmarked_data <- benchmarked_data %>%
    mutate(
      On_Factor = 0.5 / (On_Calls / (On_Calls + Off_Calls)),
      Off_Factor = 0.5 / (Off_Calls / (On_Calls + Off_Calls))
    ) %>%
    select(Call_Year, Call_Month, Call_Week, Call_Date, Skill, On_Calls, Off_Calls, On_Sales, Off_Sales, Total_Sales,
           On_CR, Off_CR, On_STD, Off_STD, Inc_Sales, On_Factor, Off_Factor) %>%
    arrange(Skill, Call_Date)
}


#reporting by day
bias_analysis_sale_per_day <- function(per_day_per_skill_data) {
  per_day_per_skill_data %>%
    group_by(Call_Year, Call_Month, Call_Week, Call_Date) %>%
    summarise(
      Incrementals = sum(Inc_Sales),
      gain = (sum(Inc_Sales) / (sum(On_Sales) -  sum(Inc_Sales))) * 100.0,
      On_Expected = sum(On_Sales * On_Factor) / sum(On_Calls * On_Factor) ,
      Off_Expected = sum(Off_Sales * Off_Factor) / sum(Off_Calls * Off_Factor) ,
      On_Stdev = sqrt(sum(On_STD * On_STD * On_Calls * On_Factor * On_Factor)) / sum(On_Calls * On_Factor),
      Off_Stdev = sqrt(sum(Off_STD * Off_STD * Off_Calls * Off_Factor * Off_Factor)) / sum(Off_Calls * Off_Factor),
      On_Calls = sum(On_Calls),
      Off_Calls = sum(Off_Calls),
      On_Sales = sum(On_Sales),
      Off_Sales = sum(Off_Sales),
      On_CR = On_Sales / On_Calls,
      Off_CR = Off_Sales / Off_Calls
    ) %>%
    mutate(
      SE= (100 * ( (On_Expected / Off_Expected) * sqrt( (On_Stdev / On_Expected)^2 + (Off_Stdev / Off_Expected)^ 2) ) ),
      z_score = gain / SE,
      p_value = 2 * pnorm(-abs(z_score))
    ) %>%
    select(Call_Year, Call_Month, Call_Week, Call_Date, On_Calls, Off_Calls, On_Sales, Off_Sales, On_CR, Off_CR,
           Incrementals, gain, SE, z_score, p_value)
}


#reporting by week
bias_analysis_sale_per_week <- function(per_day_per_skill_data) {
  per_day_per_skill_data %>%
    group_by(Call_Year, Call_Month, Call_Week) %>%
    summarise(
      Incrementals = sum(Inc_Sales),
      gain = (sum(Inc_Sales) / (sum(On_Sales) -  sum(Inc_Sales))) * 100.0,
      On_Expected = sum(On_Sales * On_Factor) / sum(On_Calls * On_Factor) ,
      Off_Expected = sum(Off_Sales * Off_Factor) / sum(Off_Calls * Off_Factor) ,
      On_Stdev = sqrt(sum(On_STD * On_STD * On_Calls * On_Factor * On_Factor)) / sum(On_Calls * On_Factor),
      Off_Stdev = sqrt(sum(Off_STD * Off_STD * Off_Calls * Off_Factor * Off_Factor)) / sum(Off_Calls * Off_Factor),
      On_Calls = sum(On_Calls),
      Off_Calls = sum(Off_Calls),
      On_Sales = sum(On_Sales),
      Off_Sales = sum(Off_Sales),
      On_CR = On_Sales / On_Calls,
      Off_CR = Off_Sales / Off_Calls
    ) %>%
    mutate(
      SE= (100 * ( (On_Expected / Off_Expected) * sqrt( (On_Stdev / On_Expected)^2 + (Off_Stdev / Off_Expected)^ 2) ) ),
      z_score = gain / SE,
      p_value = 2 * pnorm(-abs(z_score))
    ) %>%
    select(Call_Year, Call_Month, Call_Week, On_Calls, Off_Calls, On_Sales, Off_Sales, On_CR, Off_CR, Incrementals, gain,
           SE, z_score, p_value)
}


#reporting by month
bias_analysis_sale_per_month <- function(per_day_per_skill_data) {
  per_day_per_skill_data %>%
    group_by(Call_Year, Call_Month) %>%
    summarise(
      Incrementals = sum(Inc_Sales) ,
      gain = (sum(Inc_Sales) / (sum(On_Sales) -  sum(Inc_Sales))) * 100.0,
      On_Expected = sum(On_Sales * On_Factor) / sum(On_Calls * On_Factor) ,
      Off_Expected = sum(Off_Sales * Off_Factor) / sum(Off_Calls * Off_Factor) ,
      On_Stdev = sqrt(sum(On_STD * On_STD * On_Calls * On_Factor * On_Factor)) / sum(On_Calls * On_Factor),
      Off_Stdev = sqrt(sum(Off_STD * Off_STD * Off_Calls * Off_Factor * Off_Factor)) / sum(Off_Calls * Off_Factor),
      On_Calls = sum(On_Calls),
      Off_Calls = sum(Off_Calls),
      On_Sales = sum(On_Sales),
      Off_Sales = sum(Off_Sales),
      On_CR = On_Sales / On_Calls,
      Off_CR = Off_Sales / Off_Calls
    ) %>%
    mutate(
      SE= (100 * ( (On_Expected / Off_Expected) * sqrt( (On_Stdev / On_Expected)^2 + (Off_Stdev / Off_Expected)^ 2) ) ),
      z_score = gain / SE,
      p_value = 2 * pnorm(-abs(z_score))
    ) %>%
    select(Call_Year, Call_Month, On_Calls, Off_Calls, On_Sales, Off_Sales, On_CR, Off_CR, Incrementals, gain,
           SE, z_score, p_value)
}

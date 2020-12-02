
#aggregate for the day/skill

bias_analysis_rgu_per_day_per_skill <- function(benchmarked_data, percetOn) {
  benchmarked_data <- benchmarked_data %>%
    group_by(Call_Year, Call_Month, Call_Week, Call_Date, Skill) %>%
    summarise(
      On_Calls = n_distinct(Call_Id[on_off == 1]),
      Off_Calls = n_distinct(Call_Id[on_off == 0]),
      Total_Calls = n_distinct(Call_Id),
      On_RGUs = sum(RGU[on_off == 1]),
      Off_RGUs = sum(RGU[on_off == 0]),
      Total_RGUs = sum(RGU),
      On_Yield = sum(RGU[on_off == 1]) / n_distinct(Call_Id[on_off == 1]),
      Off_Yield = sum(RGU[on_off == 0]) / n_distinct(Call_Id[on_off == 0]),
      On_STD = sd(RGU[on_off == 1]),
      Off_STD = sd(RGU[on_off == 0])
    ) %>%
    mutate(Inc_RGUs = (On_RGUs / On_Calls -
                          Off_RGUs / Off_Calls) * On_Calls)
  benchmarked_data <- benchmarked_data %>%
    mutate(
      On_Factor = 0.5 / (On_Calls / (On_Calls + Off_Calls)),
      Off_Factor = 0.5 / (Off_Calls / (On_Calls + Off_Calls))
    ) %>%
    select(Call_Year, Call_Month, Call_Week, Call_Date, Skill, On_Calls, Off_Calls, On_RGUs, Off_RGUs, Total_RGUs,
           On_Yield, Off_Yield, On_STD, Off_STD, Inc_RGUs, On_Factor, Off_Factor) %>%
    arrange(Skill, Call_Date)
}


#reporting by day
bias_analysis_rgu_per_day <- function(per_day_per_skill_data) {
  per_day_per_skill_data %>%
    group_by(Call_Year, Call_Month, Call_Week, Call_Date) %>%
    summarise(
      Incrementals = sum(Inc_RGUs),
      gain = (sum(Inc_RGUs) / (sum(On_RGUs) -  sum(Inc_RGUs))) * 100.0,
      On_Expected = sum(On_RGUs * On_Factor) / sum(On_Calls * On_Factor) ,
      Off_Expected = sum(Off_RGUs * Off_Factor) / sum(Off_Calls * Off_Factor) ,
      On_Stdev = sqrt(sum(On_STD * On_STD * On_Calls * On_Factor * On_Factor)) / sum(On_Calls * On_Factor),
      Off_Stdev = sqrt(sum(Off_STD * Off_STD * Off_Calls * Off_Factor * Off_Factor)) / sum(Off_Calls * Off_Factor),
      On_Calls = sum(On_Calls),
      Off_Calls = sum(Off_Calls),
      On_RGUs = sum(On_RGUs),
      Off_RGUs = sum(Off_RGUs),
      On_Yield = On_RGUs / On_Calls,
      Off_Yield = Off_RGUs / Off_Calls
    ) %>%
    mutate(
      SE = (100 * ( (On_Expected / Off_Expected) * sqrt( (On_Stdev / On_Expected)^2 + (Off_Stdev / Off_Expected)^ 2) ) ),
      z_score = gain / SE,
      p_value = 2 * pnorm(-abs(z_score))
    ) %>%
    select(Call_Year, Call_Month, Call_Week, Call_Date, On_Calls, Off_Calls, On_RGUs, Off_RGUs, On_Yield, Off_Yield,
           Incrementals, gain, SE, z_score, p_value)
}


#reporting by week
bias_analysis_rgu_per_week <- function(per_day_per_skill_data) {
  per_day_per_skill_data %>%
    group_by(Call_Year, Call_Month, Call_Week) %>%
    summarise(
      Incrementals = sum(Inc_RGUs),
      gain = (sum(Inc_RGUs) / (sum(On_RGUs) -  sum(Inc_RGUs))) * 100.0,
      On_Expected = sum(On_RGUs * On_Factor) / sum(On_Calls * On_Factor) ,
      Off_Expected = sum(Off_RGUs * Off_Factor) / sum(Off_Calls * Off_Factor) ,
      On_Stdev = sqrt(sum(On_STD * On_STD * On_Calls * On_Factor * On_Factor)) / sum(On_Calls * On_Factor),
      Off_Stdev = sqrt(sum(Off_STD * Off_STD * Off_Calls * Off_Factor * Off_Factor)) / sum(Off_Calls * Off_Factor),
      On_Calls = sum(On_Calls),
      Off_Calls = sum(Off_Calls),
      On_RGUs = sum(On_RGUs),
      Off_RGUs = sum(Off_RGUs),
      On_Yield = On_RGUs / On_Calls,
      Off_Yield = Off_RGUs / Off_Calls
    ) %>%
    mutate(
      SE = (100 * ( (On_Expected / Off_Expected) * sqrt( (On_Stdev / On_Expected)^2 + (Off_Stdev / Off_Expected)^ 2) ) ),
      z_score = gain / SE,
      p_value = 2 * pnorm(-abs(z_score))
    ) %>%
    select(Call_Year, Call_Month, Call_Week, On_Calls, Off_Calls, On_RGUs, Off_RGUs, On_Yield, Off_Yield, Incrementals, gain,
           SE, z_score, p_value)
}


#reporting by month
bias_analysis_rgu_per_month <- function(per_day_per_skill_data) {
  per_day_per_skill_data %>%
    group_by(Call_Year, Call_Month) %>%
    summarise(
      Incrementals = sum(Inc_RGUs),
      gain = (sum(Inc_RGUs) / (sum(On_RGUs) -  sum(Inc_RGUs))) * 100.0,
      On_Expected = sum(On_RGUs * On_Factor) / sum(On_Calls * On_Factor) ,
      Off_Expected = sum(Off_RGUs * Off_Factor) / sum(Off_Calls * Off_Factor) ,
      On_Stdev = sqrt(sum(On_STD * On_STD * On_Calls * On_Factor * On_Factor)) / sum(On_Calls * On_Factor),
      Off_Stdev = sqrt(sum(Off_STD * Off_STD * Off_Calls * Off_Factor * Off_Factor)) / sum(Off_Calls * Off_Factor),
      On_Calls = sum(On_Calls),
      Off_Calls = sum(Off_Calls),
      On_RGUs = sum(On_RGUs),
      Off_RGUs = sum(Off_RGUs),
      On_Yield = On_RGUs / On_Calls,
      Off_Yield = Off_RGUs / Off_Calls
    ) %>%
    mutate(
      SE = (100 * ( (On_Expected / Off_Expected) * sqrt( (On_Stdev / On_Expected)^2 + (Off_Stdev / Off_Expected)^ 2) ) ),
      z_score = gain / SE,
      p_value = 2 * pnorm(-abs(z_score))
    ) %>%
    select(Call_Year, Call_Month, On_Calls, Off_Calls, On_RGUs, Off_RGUs, On_Yield, Off_Yield, Incrementals, gain,
           SE, z_score, p_value)
}

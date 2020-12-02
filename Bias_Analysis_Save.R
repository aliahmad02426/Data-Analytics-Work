
#aggregate for the day/skill

bias_analysis_save_per_day_per_skill <- function(benchmarked_data, percetOn) {
  benchmarked_data <- benchmarked_data %>%
    group_by(Call_Year, Call_Month, Call_Week, Call_Date, Skill) %>%
    summarise(
      On_Calls = n_distinct(Call_Id[on_off == 1]),
      Off_Calls = n_distinct(Call_Id[on_off == 0]),
      Total_Calls = n_distinct(Call_Id),
      On_Saves = sum(Save[on_off == 1]),
      Off_Saves = sum(Save[on_off == 0]),
      Total_Saves = sum(Save),
      On_SR = sum(Save[on_off == 1]) / n_distinct(Call_Id[on_off == 1]),
      Off_SR = sum(Save[on_off == 0]) / n_distinct(Call_Id[on_off == 0]),
      On_STD = sd(Save[on_off == 1]),
      Off_STD = sd(Save[on_off == 0])
    ) %>%
    mutate(Inc_Saves = (On_Saves / On_Calls -
                          Off_Saves / Off_Calls) * On_Calls)
  benchmarked_data <- benchmarked_data %>%
    mutate(
      On_Factor = 0.5 / (On_Calls / (On_Calls + Off_Calls)),
      Off_Factor = 0.5 / (Off_Calls / (On_Calls + Off_Calls))
    ) %>%
    select(Call_Year, Call_Month, Call_Week, Call_Date, Skill, On_Calls, Off_Calls, On_Saves, Off_Saves, Total_Saves,
           On_SR, Off_SR, On_STD, Off_STD, Inc_Saves, On_Factor, Off_Factor) %>%
    arrange(Skill, Call_Date)
}
 

#reporting by day
bias_analysis_save_per_day <- function(per_day_per_skill_data) {
  per_day_per_skill_data %>%
    group_by(Call_Year, Call_Month, Call_Week, Call_Date) %>%
    summarise(
      Incrementals = sum(Inc_Saves),
      gain = (sum(Inc_Saves) / (sum(On_Saves) -  sum(Inc_Saves))) * 100.0,
      On_Expected = sum(On_Saves * On_Factor) / sum(On_Calls * On_Factor) ,
      Off_Expected = sum(Off_Saves * Off_Factor) / sum(Off_Calls * Off_Factor) ,
      On_Stdev = sqrt(sum(On_STD * On_STD * On_Calls * On_Factor * On_Factor)) / sum(On_Calls * On_Factor),
      Off_Stdev = sqrt(sum(Off_STD * Off_STD * Off_Calls * Off_Factor * Off_Factor)) / sum(Off_Calls * Off_Factor),
      On_Calls = sum(On_Calls),
      Off_Calls = sum(Off_Calls),
      On_Saves = sum(On_Saves),
      Off_Saves = sum(Off_Saves),
      On_CR = On_Saves / On_Calls,
      Off_CR = Off_Saves / Off_Calls
    ) %>%  
    mutate(
      SE= (100 * ( (On_Expected / Off_Expected) * sqrt( (On_Stdev / On_Expected)^2 + (Off_Stdev / Off_Expected)^ 2) ) ),
      z_score = gain / SE, 
      p_value = 2 * pnorm(-abs(z_score))
    ) %>% 
    select(Call_Year, Call_Month, Call_Week, Call_Date, On_Calls, Off_Calls, On_Saves, Off_Saves, On_CR, Off_CR, 
           Incrementals, gain, SE, z_score, p_value)
}


#reporting by week
bias_analysis_save_per_week <- function(per_day_per_skill_data) {
  per_day_per_skill_data %>%
    group_by(Call_Year, Call_Month, Call_Week) %>%
    summarise(
      Incrementals = sum(Inc_Saves),
      gain = (sum(Inc_Saves) / (sum(On_Saves) -  sum(Inc_Saves))) * 100.0,
      On_Expected = sum(On_Saves * On_Factor) / sum(On_Calls * On_Factor) ,
      Off_Expected = sum(Off_Saves * Off_Factor) / sum(Off_Calls * Off_Factor) ,
      On_Stdev = sqrt(sum(On_STD * On_STD * On_Calls * On_Factor * On_Factor)) / sum(On_Calls * On_Factor),
      Off_Stdev = sqrt(sum(Off_STD * Off_STD * Off_Calls * Off_Factor * Off_Factor)) / sum(Off_Calls * Off_Factor),
      On_Calls = sum(On_Calls),
      Off_Calls = sum(Off_Calls),
      On_Saves = sum(On_Saves),
      Off_Saves = sum(Off_Saves),
      On_CR = On_Saves / On_Calls,
      Off_CR = Off_Saves / Off_Calls
    ) %>%  
    mutate(
      SE= (100 * ( (On_Expected / Off_Expected) * sqrt( (On_Stdev / On_Expected)^2 + (Off_Stdev / Off_Expected)^ 2) ) ),
      z_score = gain / SE, 
      p_value = 2 * pnorm(-abs(z_score))
    ) %>% 
    select(Call_Year, Call_Month, Call_Week, On_Calls, Off_Calls, On_Saves, Off_Saves, On_CR, Off_CR, Incrementals, gain, 
           SE, z_score, p_value)
}


#reporting by month
bias_analysis_save_per_month <- function(per_day_per_skill_data) {
  per_day_per_skill_data %>%
    group_by(Call_Year, Call_Month) %>%
    summarise(
      Incrementals = sum(Inc_Saves),
      gain = (sum(Inc_Saves) / (sum(On_Saves) -  sum(Inc_Saves))) * 100.0,
      On_Expected = sum(On_Saves * On_Factor) / sum(On_Calls * On_Factor) ,
      Off_Expected = sum(Off_Saves * Off_Factor) / sum(Off_Calls * Off_Factor) ,
      On_Stdev = sqrt(sum(On_STD * On_STD * On_Calls * On_Factor * On_Factor)) / sum(On_Calls * On_Factor),
      Off_Stdev = sqrt(sum(Off_STD * Off_STD * Off_Calls * Off_Factor * Off_Factor)) / sum(Off_Calls * Off_Factor),
      On_Calls = sum(On_Calls),
      Off_Calls = sum(Off_Calls),
      On_Saves = sum(On_Saves),
      Off_Saves = sum(Off_Saves),
      On_CR = On_Saves / On_Calls,
      Off_CR = Off_Saves / Off_Calls
    ) %>%  
    mutate(
      SE= (100 * ( (On_Expected / Off_Expected) * sqrt( (On_Stdev / On_Expected)^2 + (Off_Stdev / Off_Expected)^ 2) ) ),
      z_score = gain / SE, 
      p_value = 2 * pnorm(-abs(z_score))
    ) %>% 
    select(Call_Year, Call_Month, On_Calls, Off_Calls, On_Saves, Off_Saves, On_CR, Off_CR, Incrementals, gain, 
           SE, z_score, p_value)
}

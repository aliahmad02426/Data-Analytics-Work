
#aggregate for the day/skill

bias_analysis_ratio_per_day_per_skill <- function(benchmarked_data, percetOn) {
  benchmarked_data <- benchmarked_data %>%
    group_by(Call_Year, Call_Month, Call_Week, Call_Date, Skill) %>%
    summarise(
      On_Calls = n_distinct(Call_Id[on_off == 1]),
      Off_Calls = n_distinct(Call_Id[on_off == 0]),
      Total_Calls = n_distinct(Call_Id),
      On_Revenue = sum(Revenue[on_off == 1]),
      Off_Revenue = sum(Revenue[on_off == 0]),
      Total_Revenue = sum(Revenue),
      On_PreCallRevenue = sum(PreCallRevenue[on_off == 1]),
      Off_PreCallRevenue = sum(PreCallRevenue[on_off == 0]),
      On_R = On_Revenue / On_PreCallRevenue,
      Off_R = Off_Revenue / Off_PreCallRevenue,
      On_STD = 1/(On_PreCallRevenue/On_Calls) * 
        sqrt(  sum( (
          Revenue[on_off == 1] - On_R * PreCallRevenue[on_off == 1]  )^2) / (On_Calls-1)),
      Off_STD = 1/(Off_PreCallRevenue / Off_Calls) * 
        sqrt(  sum( (
          Revenue[on_off == 0] - Off_R * PreCallRevenue[on_off == 0]  )^2) / (Off_Calls-1))
      
    ) %>%
    mutate(
      Inc_Revenue = (On_R - Off_R) * On_PreCallRevenue
    )
  benchmarked_data <- benchmarked_data %>%
    mutate(
      On_Factor = 0.5 / (On_Calls / (On_Calls + Off_Calls)),
      Off_Factor = 0.5 / (Off_Calls / (On_Calls + Off_Calls))
    ) %>%
    select(Call_Year, Call_Month, Call_Week, Call_Date, Skill, On_Calls, Off_Calls, On_Revenue, Off_Revenue, 
           On_PreCallRevenue, Off_PreCallRevenue, On_R, Off_R, On_STD, Off_STD, 
           Inc_Revenue, On_Factor, Off_Factor) %>%
    arrange(Skill, Call_Date)
}


#reporting by day
bias_analysis_ratio_per_day <- function(per_day_per_skill_data) {
  per_day_per_skill_data %>%
    group_by(Call_Year, Call_Month, Call_Week, Call_Date) %>%
    summarise(
      Incrementals = sum(Inc_Revenue),
      gain = (sum(Inc_Revenue) / (sum(On_Revenue) - sum(Inc_Revenue) )) * 100.0,
      On_Expected = sum(On_Revenue * On_Factor) / sum(On_PreCallRevenue * On_Factor),
      Off_Expected = sum(Off_Revenue * Off_Factor) / sum(Off_PreCallRevenue * Off_Factor),
      On_Stdev = sqrt(sum(On_STD * On_STD * On_Calls * On_Factor * On_Factor)) / sum(On_Calls * On_Factor) ,
      Off_Stdev = sqrt(sum(Off_STD * Off_STD * Off_Calls * Off_Factor * Off_Factor)) / sum(Off_Calls * Off_Factor),
      On_Calls = sum(On_Calls),
      Off_Calls = sum(Off_Calls),
      On_Revenue = sum(On_Revenue),
      Off_Revenue = sum(Off_Revenue),
      On_PreCallRevenue = sum(On_PreCallRevenue),
      Off_PreCallRevenue = sum(Off_PreCallRevenue),
      On_R = On_Revenue / On_PreCallRevenue,
      Off_R = Off_Revenue / Off_PreCallRevenue
    ) %>%
    mutate(
      SE = (100 * ( (On_Expected / Off_Expected) * sqrt( (On_Stdev / On_Expected)^2 + (Off_Stdev / Off_Expected)^ 2) ) ),
      z_score = gain / SE, 
      p_value = 2 * pnorm(-abs(z_score))
    ) %>% 
    select(Call_Year, Call_Month, Call_Week, Call_Date, On_Calls, Off_Calls, On_Revenue, Off_Revenue, 
           On_PreCallRevenue, Off_PreCallRevenue, On_R, Off_R, Incrementals, gain, SE, z_score, p_value)
}

#reporting by week
bias_analysis_ratio_per_week <- function(per_day_per_skill_data) {
  per_day_per_skill_data %>%
    group_by(Call_Year, Call_Month, Call_Week) %>%
    summarise(
      Incrementals = sum(Inc_Revenue),
      gain = (sum(Inc_Revenue) / (sum(On_Revenue) - sum(Inc_Revenue) )) * 100.0,
      On_Expected = sum(On_Revenue * On_Factor) / sum(On_PreCallRevenue * On_Factor),
      Off_Expected = sum(Off_Revenue * Off_Factor) / sum(Off_PreCallRevenue * Off_Factor),
      On_Stdev = sqrt(sum(On_STD * On_STD * On_Calls * On_Factor * On_Factor)) / sum(On_Calls * On_Factor) ,
      Off_Stdev = sqrt(sum(Off_STD * Off_STD * Off_Calls * Off_Factor * Off_Factor)) / sum(Off_Calls * Off_Factor),
      On_Calls = sum(On_Calls),
      Off_Calls = sum(Off_Calls),
      On_Revenue = sum(On_Revenue),
      Off_Revenue = sum(Off_Revenue),
      On_PreCallRevenue = sum(On_PreCallRevenue),
      Off_PreCallRevenue = sum(Off_PreCallRevenue),
      On_R = On_Revenue / On_PreCallRevenue,
      Off_R = Off_Revenue / Off_PreCallRevenue
    ) %>%
    mutate(
      SE = (100 * ( (On_Expected / Off_Expected) * sqrt( (On_Stdev / On_Expected)^2 + (Off_Stdev / Off_Expected)^ 2) ) ),
      z_score = gain / SE, 
      p_value = 2 * pnorm(-abs(z_score))
    ) %>% 
    select(Call_Year, Call_Month, Call_Week, On_Calls, Off_Calls, On_Revenue, Off_Revenue, On_PreCallRevenue, Off_PreCallRevenue,
           On_R, Off_R, Incrementals, gain, SE, z_score, p_value)
}


#reporting by month
bias_analysis_ratio_per_month <- function(per_day_per_skill_data) {
  per_day_per_skill_data %>%
    group_by(Call_Year, Call_Month) %>%
    summarise(
      Incrementals = sum(Inc_Revenue),
      gain = (sum(Inc_Revenue) / (sum(On_Revenue) - sum(Inc_Revenue) )) * 100.0,
      On_Expected = sum(On_Revenue * On_Factor) / sum(On_PreCallRevenue * On_Factor),
      Off_Expected = sum(Off_Revenue * Off_Factor) / sum(Off_PreCallRevenue * Off_Factor),
      On_Stdev = sqrt(sum(On_STD * On_STD * On_Calls * On_Factor * On_Factor)) / sum(On_Calls * On_Factor) ,
      Off_Stdev = sqrt(sum(Off_STD * Off_STD * Off_Calls * Off_Factor * Off_Factor)) / sum(Off_Calls * Off_Factor),
      On_Calls = sum(On_Calls),
      Off_Calls = sum(Off_Calls),
      On_Revenue = sum(On_Revenue),
      Off_Revenue = sum(Off_Revenue),
      On_PreCallRevenue = sum(On_PreCallRevenue),
      Off_PreCallRevenue = sum(Off_PreCallRevenue),
      On_R = On_Revenue / On_PreCallRevenue,
      Off_R = Off_Revenue / Off_PreCallRevenue
    ) %>%
    mutate(
      SE= (100 * ( (On_Expected / Off_Expected) * sqrt( (On_Stdev / On_Expected)^2 + (Off_Stdev / Off_Expected)^ 2) ) ),
      z_score = gain / SE, 
      p_value = 2 * pnorm(-abs(z_score))
    ) %>% 
    select(Call_Year, Call_Month, On_Calls, Off_Calls, On_Revenue, Off_Revenue, On_PreCallRevenue, Off_PreCallRevenue,
           On_R, Off_R, Incrementals, gain, SE, z_score, p_value)
  
}

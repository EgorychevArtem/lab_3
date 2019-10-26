package com.examples.Laba;

import java.io.Serializable;

public class CalculateStatistic implements Serializable {
    float maxDelay;
    int Flightscount, DelayFlights, CancelFlights;

    public CalculateStatistic(int Flightscount, int CancelFlights, int DelayFlights, float maxDelay) {
        this.maxDelay = maxDelay;
        this.Flightscount = Flightscount;
        this.CancelFlights = CancelFlights;
        this.DelayFlights = DelayFlights;
    }

    public static CalculateStatistic AddNewValue(CalculateStatistic stat, boolean isCancel, boolean isDelay, float delay){
      return new CalculateStatistic(stat.Flightscount + 1,
                                    isCancel ? stat.CancelFlights + 1 : stat.CancelFlights,
                                    isDelay ? stat.DelayFlights + 1 : stat.DelayFlights,
                                    delay > stat.maxDelay ? delay : stat.maxDelay );
    }

    public static CalculateStatistic add(CalculateStatistic f, CalculateStatistic s){
        return new CalculateStatistic(f.Flightscount + s.Flightscount,
                                     f.CancelFlights + s.CancelFlights,
                                      f.DelayFlights + s.DelayFlights,
                                        f.maxDelay + s.maxDelay);
    }

    public static float getCountPercent(float first, float second){
        return first / second * 100.0f;
    }

    public static String PrintResult(CalculateStatistic stat){
        float DelayPercent = getCountPercent(stat.DelayFlights,stat.Flightscount);
        float CancelFlightsPercent = getCountPercent(stat.CancelFlights, stat.Flightscount);
        return "| Max delay time: " + stat.maxDelay + "  | Delayed flights: " + DelayPercent + "%; " +  " | Cancelled flights: " + CancelFlightsPercent + "%; ";
    }
}
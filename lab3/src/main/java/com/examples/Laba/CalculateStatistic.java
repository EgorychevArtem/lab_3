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
}

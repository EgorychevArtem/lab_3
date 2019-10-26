package com.examples.Laba;

import java.io.Serializable;

public class FlightData implements Serializable {
    int AiroportID, DestAiroportID;
    float Delay, CancelledFlight;

    public FlightData() {}

    public FlightData(int AiroportID, int DestAiroportID, float Delay, float CancelledFlight){
        this.AiroportID = AiroportID;
        this.DestAiroportID = DestAiroportID;
        this.Delay = Delay;
        this.CancelledFlight = CancelledFlight;
    }
}


package com.examples.Laba;

import java.io.Serializable;

public class Serializabl implements Serializable {
    int AiroportID, DestAiroportID;
    float Delay, CancelledFlight;

    public Serializabl() {}

    public Serializabl(int AiroportID, int DestAiroportID, float Delay, float CancelledFlight){
        this.AiroportID = AiroportID;
        this.DestAiroportID = DestAiroportID;
        this.Delay = Delay;
        this.CancelledFlight = CancelledFlight;
    }

    @Override
    public String toString() {
        return "FlightSerializable{" +
                "originAirportID=" + AiroportID +
                ", destAirportID=" + DestAiroportID +
                ", delayTime=" + Delay +
                ", cancelled=" + CancelledFlight +
                '}';
    }
}

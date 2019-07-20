package com.datafibers.kafka.connect;

import org.json.JSONObject;
import ucar.ma2.ArrayFloat;
import ucar.ma2.ArrayLong;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Test
 */
public class NETCDFHelper {

    public static void getAirPressure(String topic, String fileName) {
        getAirPressure(topic, fileName, 0);
    }

    public static void getAirPressure(String topic, String fileName, int realization) {
        try {
            NetcdfFile dataFile = NetcdfFile.open(fileName, null);
            // read all variables
            Variable pressureVar = dataFile.findVariable("surface_air_pressure");
            Variable yVar = dataFile.findVariable("projection_y_coordinate");
            Variable xVar = dataFile.findVariable("projection_x_coordinate");

            // Read the valid time and forecast time
            Variable time = dataFile.findVariable("time");
            ArrayLong.D0 timeCoord = (ArrayLong.D0) time.read();

            Variable forecastTime = dataFile.findVariable("forecast_reference_time");
            ArrayLong.D0 forecastTimeCoord = (ArrayLong.D0) forecastTime.read();

            // Read the latitude and longitude coordinate variables into arrays
            ArrayFloat.D1 yCoord = (ArrayFloat.D1) yVar.read();
            ArrayFloat.D1 xCoord = (ArrayFloat.D1) xVar.read();

            // Read the pressure data
            ArrayFloat.D3 pressure = (ArrayFloat.D3) pressureVar.read();

            // Get dimensions of the y/x array
            List<Dimension> dimensions = dataFile.getDimensions();
            int yLength = dimensions.get(1).getLength();
            int xLength = dimensions.get(2).getLength();

            // iterate through the arrays, do something with the data
            for (int y = 0; y < yLength; y++) {
                for (int x = 0; x < xLength; x++) {
                    // do something with the data
                    System.out.print(topic + "," + yCoord.get(y) + ",");
                    System.out.print(xCoord.get(x) + ",");
                    System.out.print(timeCoord.get() + ",");
                    System.out.print(forecastTimeCoord.get() + ",");
                    System.out.println(pressure.get(realization, y, x) + ",");
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String [] args) throws Exception{

        //String fileName = "SkyWise-Global7km-GFSAviation_20170120_1200.nc";
        String fileName = "surface_air_pressure_2019-07-18T23:26:50Z_abf8f33bd33bb2bf8ffe18ddeb992423d532fad9.nc";
        NetcdfFile dataFile = NetcdfFile.open(fileName, null);

        // print out the file structure
        System.out.println(dataFile.toString());
        System.out.println(dataFile.getDimensions());

        // read the lat/lon and temperature variables
        Variable pressureVar = dataFile.findVariable("surface_air_pressure");
        Variable realizationVar = dataFile.findVariable("realization");
        Variable yVar = dataFile.findVariable("projection_y_coordinate");
        Variable xVar = dataFile.findVariable("projection_x_coordinate");

        // Read the units for the temperature variable
        System.out.println("units: " + pressureVar.findAttributeIgnoreCase("units"));

        // Read the valid time for this data
        Variable time = dataFile.findVariable("time");
        System.out.println("valid time: " + time.findAttributeIgnoreCase("units"));
        ArrayLong.D0 timeCoord = (ArrayLong.D0) time.read();

        // if it's a forecast, get the forecast reference time
        Variable forecastTime = dataFile.findVariable("forecast_reference_time");
        System.out.println("forecast time: " + forecastTime.findAttributeIgnoreCase("units"));
        ArrayLong.D0 forecastTimeCoord = (ArrayLong.D0) forecastTime.read();

        // Read the latitude and longitude coordinate variables into arrays
        ArrayFloat.D1 yCoord = (ArrayFloat.D1) yVar.read();
        ArrayFloat.D1 xCoord = (ArrayFloat.D1) xVar.read();

        // Read the turbulence & edr data
        ArrayFloat.D3 pressure = (ArrayFloat.D3) pressureVar.read();

        // dimensions of the lat/lon array
        List<Dimension> dimensions = dataFile.getDimensions();
        int rLength = dimensions.get(0).getLength();
        int yLength = dimensions.get(1).getLength();
        int xLength = dimensions.get(2).getLength();

        System.out.println("r length: " + rLength);
        System.out.println("y coord. length: " + yLength);
        System.out.println("x coord. length: " + xLength);

        // iterate through the arrays, do something with the data
        for (int y = 0; y < yLength; y++) {
            for (int x = 0; x < xLength; x++) {
                // do something with the data
                System.out.print(yCoord.get(y) + ",");
                System.out.print(xCoord.get(x) + ",");
                System.out.print(timeCoord.get() + ",");
                System.out.print(forecastTimeCoord.get() + ",");
                System.out.println(pressure.get(0, y, x) + ",");
            }
        }
    }
}

package com.datafibers.kafka.connect;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.mashape.unirest.request.GetRequest;
import org.apache.avro.SchemaParseException;
import org.apache.http.HttpHost;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import org.codehaus.jackson.map.ObjectMapper;
import yahoofinance.Stock;
import yahoofinance.YahooFinance;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.URL;

import static org.apache.avro.Schema.Type.RECORD;

/**
 * Test
 */
public class StockTest {

    static void getStock(String restURL) {

        String responseString;
        BufferedReader br = null;

        try {
            StringBuilder response = new StringBuilder();
            String line;
            br = new BufferedReader(new InputStreamReader(new URL(restURL).openStream()));

            while ((line = br.readLine()) != null) {
                response.append(line);
            }

            org.codehaus.jackson.JsonNode responseJson = new ObjectMapper().readValue(response.toString(), org.codehaus.jackson.JsonNode.class);
            responseString = responseJson.get("schema").asText();
            System.out.println("ack - " + responseString);
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            try {
                if (br != null)
                    br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String [] args) {

        String apiURL = "https://globalrealtime.xignite.com/v3/xGlobalRealTime.json/ListExchanges?";
        getStock(apiURL);
    }
}

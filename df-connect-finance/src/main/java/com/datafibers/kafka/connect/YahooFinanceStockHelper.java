package com.datafibers.kafka.connect;

import org.json.JSONObject;
import yahoofinance.Stock;
import yahoofinance.YahooFinance;
import yahoofinance.histquotes.Interval;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Test
 */
public class YahooFinanceStockHelper {

    public static final Map<String, String> portfolio;

    static {
        portfolio = new HashMap<>();
        portfolio.put("Top 10 IT Service", "ACN,CTSH,EPAM,GIB,DOX,SAIC,FORR,INFY,WIT,INXN");
        portfolio.put("Top 10 Technology", "GOOGL,MSFT,AMZN,BABA,ORCL,IBM,HPE,SAP,FB,EBAY");
    }

    public static String getStockJson(String symbol, Boolean refresh) {

        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        df.setTimeZone(TimeZone.getTimeZone("America/New_York"));

        try {
            Stock stock = YahooFinance.get(symbol);
            JSONObject stockJson = new JSONObject()
                    .put("refresh_time", df.format(new Date()))
                    .put("symbol", stock.getSymbol())
                    .put("company_name", stock.getName())
                    .put("exchange", stock.getStockExchange())
                    .put("open_price", stock.getQuote(refresh).getOpen().toString())
                    .put("ask_price", stock.getQuote(refresh).getAsk().toString())
                    .put("ask_size", stock.getQuote(refresh).getAskSize().toString())
                    .put("bid_price", stock.getQuote(refresh).getBid().toString())
                    .put("bid_size", stock.getQuote(refresh).getBidSize().toString())
                    .put("price",  stock.getQuote(refresh).getPrice().toString());
            return stockJson.toString();
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        return null;
    }

    public static String getFakedStockJson(String symbol, String source) {
        JSONObject stockJson = null;
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        df.setTimeZone(TimeZone.getTimeZone("America/New_York"));
        Random rand = new Random();

        if (source.equalsIgnoreCase("PAST")) {
            try {
                Stock stock = YahooFinance.get(symbol);
                // TODO need to find the 1st not null date in history and use it as spoof baseline
                stockJson = new JSONObject().put("refresh_time", df.format(new Date()))
                        .put("symbol", stock.getSymbol())
                        .put("company_name", stock.getName() == null ?
                                symbol + " Inc.":stock.getName())
                        .put("exchange", stock.getStockExchange())
                        .put("open_price", (stock.getQuote().getOpen() == null ?
                                BigDecimal.valueOf(30):stock.getQuote().getOpen())
                                .add(BigDecimal.valueOf(rand.nextInt(10))).toString())
                        .put("ask_price", (stock.getQuote().getAsk() == null ?
                                BigDecimal.valueOf(30):stock.getQuote().getAsk())
                                .add(BigDecimal.valueOf(rand.nextInt(10))).toString())
                        .put("ask_size", (stock.getQuote().getAskSize() == null ?
                                Long.valueOf(30):stock.getQuote().getAskSize())
                                + Long.valueOf(rand.nextInt(100)).toString())
                        .put("bid_price", (stock.getQuote().getBid() == null ?
                                BigDecimal.valueOf(50):stock.getQuote().getBid())
                                .add(BigDecimal.valueOf(rand.nextInt(20))).toString())
                        .put("bid_size", (stock.getQuote().getBidSize() == null ?
                                Long.valueOf(45):stock.getQuote().getBidSize())
                                + Long.valueOf(rand.nextInt(200)).toString())
                        .put("price",  (stock.getQuote().getPrice() == null ?
                                BigDecimal.valueOf(50):stock.getQuote().getPrice())
                                .add(BigDecimal.valueOf(rand.nextInt(10))).toString())
                ;

            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        } else {
            stockJson = new JSONObject().put("refresh_time", df.format(new Date()))
                    .put("symbol", symbol)
                    .put("company_name", symbol)
                    .put("exchange", symbol)
                    .put("open_price", BigDecimal.valueOf(rand.nextInt(105)).toString())
                    .put("ask_price", BigDecimal.valueOf(rand.nextInt(102)).toString())
                    .put("ask_size", Long.valueOf(rand.nextInt(130)).toString())
                    .put("bid_price", BigDecimal.valueOf(rand.nextInt(200)).toString())
                    .put("bid_size", Long.valueOf(rand.nextInt(230)).toString())
                    .put("price",  BigDecimal.valueOf(rand.nextInt(120)).toString());
        }

        return stockJson.toString();
    }

    public static void main(String [] args) {
        try {
            Calendar from = Calendar.getInstance();
            Calendar to = Calendar.getInstance();
            from.add(Calendar.MONTH, -1); // from 5 years ago

            Stock google = YahooFinance.get("BABA", from, to, Interval.DAILY);
            System.out.println(google.getHistory().get(0).getClose());

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        String[] symbols = new String[]{"INTC", "TSLA", "AIR.PA", "YHOO"};
        for (String symbol : symbols) {
            //System.out.println(YahooFinanceStockHelper.getStockJson(symbol, true));
            System.out.println(YahooFinanceStockHelper.getFakedStockJson(symbol, "PAST"));
        }
    }
}

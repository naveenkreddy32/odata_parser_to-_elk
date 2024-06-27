package com.api;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ElasticsearchParser {

    private static final Logger logger = LogManager.getLogger(ElasticsearchParser.class);

    public static String parse(String filter) {
    	logger.info("Parsing filter: {}", filter);
    	JSONObject result = parseOrExpr(new Tokenizer(filter));
        String resultString = result.toString();
        logger.info("Parsed Elasticsearch DSL: {}", resultString);
        return resultString;
    }

    private static JSONObject parseOrExpr(Tokenizer tokenizer) {
        List<JSONObject> orList = new ArrayList<>();
        do {
            orList.add(parseAndExpr(tokenizer));
        } while (tokenizer.consume("or"));

        if (orList.size() == 1) {
            return orList.get(0);
        }

        JSONObject orQuery = new JSONObject();
        orQuery.put("bool", new JSONObject().put("should", new JSONArray(orList)));
        return orQuery;
    }

    private static JSONObject parseAndExpr(Tokenizer tokenizer) {
        List<JSONObject> andList = new ArrayList<>();
        do {
            andList.add(parseComparisonExpr(tokenizer));
        } while (tokenizer.consume("and"));

        if (andList.size() == 1) {
            return andList.get(0);
        }

        JSONObject andQuery = new JSONObject();
        andQuery.put("bool", new JSONObject().put("must", new JSONArray(andList)));
        return andQuery;
    }

    private static JSONObject parseComparisonExpr(Tokenizer tokenizer) {
        if (tokenizer.consume("(")) {
            JSONObject expr = parseOrExpr(tokenizer);
            tokenizer.consume(")");
            return expr;
        }

        String left = tokenizer.next();
        String op = tokenizer.next();
        Object right = tokenizer.nextValue();

        JSONObject comparisonQuery = new JSONObject();
        switch (op) {
            case "eq":
                comparisonQuery.put("match_phrase", new JSONObject().put(left, right));
                break;
            case "ne":
                comparisonQuery.put("bool", new JSONObject().put("must_not", new JSONObject().put("match_phrase", new JSONObject().put(left, right))));
                break;
            case "gt":
                comparisonQuery.put("range", new JSONObject().put(left, new JSONObject().put("gt", right)));
                break;
            case "ge":
                comparisonQuery.put("range", new JSONObject().put(left, new JSONObject().put("gte", right)));
                break;
            case "lt":
                comparisonQuery.put("range", new JSONObject().put(left, new JSONObject().put("lt", right)));
                break;
            case "le":
                comparisonQuery.put("range", new JSONObject().put(left, new JSONObject().put("lte", right)));
                break;
            default:
                throw new IllegalArgumentException("Unsupported comparison operator: " + op);
        }
        return comparisonQuery;
    }

    private static class Tokenizer {
        private final List<String> tokens;
        private int index = 0;

        public Tokenizer(String input) {
            tokens = tokenize(input);
        }

        private List<String> tokenize(String input) {
            List<String> tokens = new ArrayList<>();
            StringBuilder token = new StringBuilder();
            boolean inQuotes = false;

            for (char ch : input.toCharArray()) {
                if (ch == '\'') {
                    inQuotes = !inQuotes;
                }
                if (Character.isWhitespace(ch) && !inQuotes) {
                    if (token.length() > 0) {
                        tokens.add(token.toString());
                        token.setLength(0);
                    }
                } else if (!inQuotes && (ch == '(' || ch == ')' || ch == ',')) {
                    if (token.length() > 0) {
                        tokens.add(token.toString());
                        token.setLength(0);
                    }
                    tokens.add(String.valueOf(ch));
                } else {
                    token.append(ch);
                }
            }

            if (token.length() > 0) {
                tokens.add(token.toString());
            }

            return tokens;
        }

        public boolean consume(String token) {
            if (index < tokens.size() && tokens.get(index).equalsIgnoreCase(token)) {
                index++;
                return true;
            }
            return false;
        }

        public String next() {
            if (index < tokens.size()) {
                return tokens.get(index++);
            }
            throw new IllegalStateException("Unexpected end of input");
        }

        public Object nextValue() {
            if (index < tokens.size()) {
                String value = tokens.get(index++);
                if (value.startsWith("'") && value.endsWith("'")) {
                    return value.substring(1, value.length() - 1);
                }
                try {
                    return Double.parseDouble(value); // Return as a number if possible
                } catch (NumberFormatException e) {
                    return value; // Return as a string if it's not a number
                }
            }
            throw new IllegalStateException("Unexpected end of input");
        }
    }
}

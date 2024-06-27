package com.api;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ElasticsearchParserTest {

    @Test
    public void testSimpleEquality() {
        String filter = "name eq 'John'";
        String expected = new JSONObject()
                .put("match_phrase", new JSONObject().put("name", "John"))
                .toString();

        String actual = ElasticsearchParser.parse(filter);
        assertEquals(expected, actual);
    }

    @Test
    public void testAndExpression() {
        String filter = "name eq 'John' and age gt 30";
        String expected = new JSONObject()
                .put("bool", new JSONObject().put("must", new JSONArray()
                        .put(new JSONObject().put("match_phrase", new JSONObject().put("name", "John")))
                        .put(new JSONObject().put("range", new JSONObject().put("age", new JSONObject().put("gt", 30))))
                ))
                .toString();

        String actual = ElasticsearchParser.parse(filter);
        assertEquals(expected, actual);
    }

    @Test
    public void testOrExpression() {
        String filter = "name eq 'John' or city eq 'New York'";
        String expected = new JSONObject()
                .put("bool", new JSONObject().put("should", new JSONArray()
                        .put(new JSONObject().put("match_phrase", new JSONObject().put("name", "John")))
                        .put(new JSONObject().put("match_phrase", new JSONObject().put("city", "New York")))
                ))
                .toString();

        String actual = ElasticsearchParser.parse(filter);
        assertEquals(expected, actual);
    }

    @Test
    public void testComplexExpression() {
        String filter = "name eq 'John' and (age gt 30 or city eq 'New York')";
        String expected = new JSONObject()
                .put("bool", new JSONObject().put("must", new JSONArray()
                        .put(new JSONObject().put("match_phrase", new JSONObject().put("name", "John")))
                        .put(new JSONObject().put("bool", new JSONObject().put("should", new JSONArray()
                                .put(new JSONObject().put("range", new JSONObject().put("age", new JSONObject().put("gt", 30))))
                                .put(new JSONObject().put("match_phrase", new JSONObject().put("city", "New York")))
                        )))
                ))
                .toString();

        String actual = ElasticsearchParser.parse(filter);
        assertEquals(expected, actual);
    }
}

package com.stratio.cassandra.index.schema;

import com.stratio.cassandra.index.schema.analysis.PreBuiltAnalyzers;
import com.stratio.cassandra.index.schema.mapping.ColumnMapper;
import com.stratio.cassandra.index.schema.mapping.ColumnMapperInteger;
import com.stratio.cassandra.index.schema.mapping.ColumnMapperString;
import com.stratio.cassandra.index.schema.mapping.ColumnMapperText;
import com.stratio.cassandra.util.JsonSerializer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.codehaus.jackson.map.JsonMappingException;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class SchemaTest {

    @Test
    public void testGetMapper() {

        Map<String, ColumnMapper> columnMappers = new HashMap<>();

        ColumnMapperString columnMapper1 = new ColumnMapperString();
        ColumnMapperString columnMapper2 = new ColumnMapperString();
        ColumnMapperString columnMapper3 = new ColumnMapperString();

        columnMappers.put("a", columnMapper1);
        columnMappers.put("a.b", columnMapper2);
        columnMappers.put("a.b.c", columnMapper3);

        Schema schema = new Schema(columnMappers, null, null);
        Assert.assertEquals(columnMapper1, schema.getMapper("a"));
        Assert.assertEquals(columnMapper2, schema.getMapper("a.b"));
        Assert.assertEquals(columnMapper3, schema.getMapper("a.b.c"));
        Assert.assertEquals(columnMapper3, schema.getMapper("a.b.c.d"));
        Assert.assertNotSame(columnMapper1, schema.getMapper("a.b.c.d"));

        schema.close();
    }

    @Test
    public void testGetDefaultAnalyzer() {
        Map<String, ColumnMapper> columnMappers = new HashMap<>();
        Schema schema = new Schema(columnMappers, null, "English");
        Analyzer analyzer = schema.getDefaultAnalyzer();
        Assert.assertEquals(EnglishAnalyzer.class, analyzer.getClass());
        schema.close();
    }

    @Test
    public void testGetDefaultAnalyzerNotSpecified() {
        Map<String, ColumnMapper> columnMappers = new HashMap<>();
        Schema schema = new Schema(columnMappers, null, null);
        Analyzer analyzer = schema.getDefaultAnalyzer();
        Assert.assertEquals(PreBuiltAnalyzers.DEFAULT.get(), analyzer);
        schema.close();
    }

    @Test
    public void testParseJSON() throws IOException {

        String json = "{" +
                      "  analyzers:{" +
                      "    spanish_analyzer : {type:\"classpath\", " +
                      "                        class:\"org.apache.lucene.analysis.es.SpanishAnalyzer\"}," +
                      "    snowball_analyzer : {type:\"snowball\", " +
                      "                         language:\"Spanish\", " +
                      "                         stopwords : \"el,la,lo,loas,las,a,ante,bajo,cabe,con,contra\"}" +
                      "  }," +
                      "  default_analyzer : \"spanish_analyzer\"," +
                      "  fields : {" +
                      "    id : {type : \"integer\"}," +
                      "    spanish_text : {type:\"text\", analyzer:\"spanish_analyzer\"}," +
                      "    snowball_text : {type:\"text\", analyzer:\"snowball_analyzer\"}" +
                      "  }" +
                      " }'";

        Schema schema = JsonSerializer.fromString(json, Schema.class);

        Analyzer defaultAnalyzer = schema.getDefaultAnalyzer();
        Assert.assertTrue(defaultAnalyzer instanceof SpanishAnalyzer);

        Analyzer spanishAnalyzer = schema.getAnalyzer("spanish_analyzer");
        Assert.assertTrue(spanishAnalyzer instanceof SpanishAnalyzer);

        ColumnMapper idMapper = schema.getMapper("id");
        Assert.assertTrue(idMapper instanceof ColumnMapperInteger);

        ColumnMapper spanishMapper = schema.getMapper("spanish_text");
        Assert.assertTrue(spanishMapper instanceof ColumnMapperText);
        Assert.assertEquals("spanish_analyzer", spanishMapper.analyzer());

        ColumnMapper snowballMapper = schema.getMapper("snowball_text");
        Assert.assertTrue(snowballMapper instanceof ColumnMapperText);
        Assert.assertEquals("snowball_analyzer", snowballMapper.analyzer());

        schema.close();
    }

    @Test
    public void testParseJSONWithNullAnalyzers() throws IOException {

        String json = "{" +
                      "  default_analyzer : \"org.apache.lucene.analysis.en.EnglishAnalyzer\"," +
                      "  fields : {" +
                      "    id : {type : \"integer\"}," +
                      "    spanish_text : {" +
                      "      type:\"text\", " +
                      "      analyzer:\"org.apache.lucene.analysis.es.SpanishAnalyzer\"}," +
                      "    snowball_text : {" +
                      "      type:\"text\", " +
                      "      analyzer:\"org.apache.lucene.analysis.en.EnglishAnalyzer\"}" +
                      "  }" +
                      " }'";

        Schema schema = JsonSerializer.fromString(json, Schema.class);

        Analyzer defaultAnalyzer = schema.getDefaultAnalyzer();
        Assert.assertTrue(defaultAnalyzer instanceof EnglishAnalyzer);

        ColumnMapper idMapper = schema.getMapper("id");
        Assert.assertTrue(idMapper instanceof ColumnMapperInteger);

        ColumnMapper spanishMapper = schema.getMapper("spanish_text");
        Assert.assertTrue(spanishMapper instanceof ColumnMapperText);
        Assert.assertEquals(SpanishAnalyzer.class.getName(), spanishMapper.analyzer());

        ColumnMapper snowballMapper = schema.getMapper("snowball_text");
        Assert.assertTrue(snowballMapper instanceof ColumnMapperText);
        Assert.assertEquals(EnglishAnalyzer.class.getName(), snowballMapper.analyzer());

        schema.close();
    }

    @Test
    public void testParseJSONWithEmptyAnalyzers() throws IOException {

        String json = "{" +
                      "  analyzers:{}, " +
                      "  default_analyzer : \"org.apache.lucene.analysis.en.EnglishAnalyzer\"," +
                      "  fields : {" +
                      "    id : {type : \"integer\"}," +
                      "    spanish_text : {type:\"text\", " +
                      "                    analyzer:\"org.apache.lucene.analysis.es.SpanishAnalyzer\"}," +
                      "    snowball_text : {type:\"text\", " +
                      "                     analyzer:\"org.apache.lucene.analysis.en.EnglishAnalyzer\"}" +
                      "  }" +
                      " }'";

        Schema schema = JsonSerializer.fromString(json, Schema.class);

        Analyzer defaultAnalyzer = schema.getDefaultAnalyzer();
        Assert.assertTrue(defaultAnalyzer instanceof EnglishAnalyzer);

        ColumnMapper idMapper = schema.getMapper("id");
        Assert.assertEquals(ColumnMapperInteger.class, idMapper.getClass());

        ColumnMapper spanishMapper = schema.getMapper("spanish_text");
        Assert.assertTrue(spanishMapper instanceof ColumnMapperText);
        Assert.assertEquals(SpanishAnalyzer.class.getName(), spanishMapper.analyzer());

        ColumnMapper snowballMapper = schema.getMapper("snowball_text");
        Assert.assertTrue(snowballMapper instanceof ColumnMapperText);
        Assert.assertEquals(EnglishAnalyzer.class.getName(), snowballMapper.analyzer());

        schema.close();
    }

    @Test
    public void testParseJSONWithNullDefaultAnalyzer() throws IOException {

        String json = "{" +
                      "  analyzers:{" +
                      "    spanish_analyzer : {" +
                      "      type:\"classpath\", " +
                      "      class:\"org.apache.lucene.analysis.es.SpanishAnalyzer\"}," +
                      "    snowball_analyzer : {" +
                      "      type:\"snowball\", " +
                      "      language:\"Spanish\", " +
                      "      stopwords : \"el,la,lo,lo,as,las,a,ante,con,contra\"}" +
                      "  }," +
                      "  fields : { id : {type : \"integer\"} }" +
                      " }'";

        Schema schema = JsonSerializer.fromString(json, Schema.class);

        Analyzer defaultAnalyzer = schema.getDefaultAnalyzer();
        Assert.assertEquals(PreBuiltAnalyzers.DEFAULT.get(), defaultAnalyzer);

        Analyzer spanishAnalyzer = schema.getAnalyzer("spanish_analyzer");
        Assert.assertTrue(spanishAnalyzer instanceof SpanishAnalyzer);

        schema.close();
    }

    @Test(expected = JsonMappingException.class)
    public void testParseJSONWithFailingDefaultAnalyzer() throws IOException {
        String json = "{default_analyzer : \"xyz\", fields : { id : {type : \"integer\"} } }'";
        JsonSerializer.fromString(json, Schema.class);
    }
}

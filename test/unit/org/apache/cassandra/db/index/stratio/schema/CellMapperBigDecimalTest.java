package org.apache.cassandra.db.index.stratio.schema;

import java.io.IOException;
import java.util.Date;
import java.util.UUID;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.junit.Assert;
import org.junit.Test;

public class CellMapperBigDecimalTest {

	@Test()
	public void testValueNull() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(10, 10);
		String parsed = mapper.indexValue("test", null);
		Assert.assertNull(parsed);
	}

	@Test
	public void testValueIntegerDigitsNull() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(null, 10);
		Assert.assertEquals(CellMapperBigDecimal.DEFAULT_INTEGER_DIGITS, mapper.getIntegerDigits());
		Assert.assertEquals(10, mapper.getDecimalDigits());
	}

	@Test
	public void testValueDecimalDigitsNull() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(10, null);
		Assert.assertEquals(10, mapper.getIntegerDigits());
		Assert.assertEquals(CellMapperBigDecimal.DEFAULT_DECIMAL_DIGITS, mapper.getDecimalDigits());
	}

	@Test
	public void testValueBothDigitsNull() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(null, null);
		Assert.assertEquals(CellMapperBigDecimal.DEFAULT_INTEGER_DIGITS, mapper.getIntegerDigits());
		Assert.assertEquals(CellMapperBigDecimal.DEFAULT_DECIMAL_DIGITS, mapper.getDecimalDigits());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueIntegerDigitsZero() {
		new CellMapperBigDecimal(0, 10);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueDecimalDigitsZero() {
		new CellMapperBigDecimal(10, 0);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueBothDigitsZero() {
		new CellMapperBigDecimal(0, 0);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueIntegerDigitsNegative() {
		new CellMapperBigDecimal(-1, 10);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueDecimalDigitsNegative() {
		new CellMapperBigDecimal(10, -1);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueBothDigitsNegative() {
		new CellMapperBigDecimal(-1, -1);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueBooleanTrue() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(100, 100);
		mapper.indexValue("test", true);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueBooleanFalse() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(100, 100);
		mapper.indexValue("test", false);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueUUID() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(100, 100);
		mapper.indexValue("test", UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueDate() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(100, 100);
		mapper.indexValue("test", new Date());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueStringInvalid() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(100, 100);
		mapper.indexValue("test", "0s0");
	}

	// /////////////

	@Test
	public void testValueStringMinPositive() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(4, 4);
		String parsed = mapper.indexValue("test", "1");
		Assert.assertEquals("10000.9999", parsed);
	}

	@Test
	public void testValueStringMaxPositive() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(4, 4);
		String parsed = mapper.indexValue("test", "9999.9999");
		Assert.assertEquals("19999.9998", parsed);
	}

	@Test
	public void testValueStringMinNegative() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(4, 4);
		String parsed = mapper.indexValue("test", "-1");
		Assert.assertEquals("09998.9999", parsed);
	}

	@Test
	public void testValueStringMaxNegative() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(4, 4);
		String parsed = mapper.indexValue("test", "-9999.9999");
		Assert.assertEquals("00000.0000", parsed);
	}

	@Test
	public void testValueStringZero() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(4, 4);
		String parsed = mapper.indexValue("test", "0");
		Assert.assertEquals("09999.9999", parsed);
	}

	@Test
	public void testValueStringLeadingZeros() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(4, 4);
		String parsed = mapper.indexValue("test", "000.042");
		Assert.assertEquals("10000.0419", parsed);
	}

	// // ///

	@Test
	public void testValueIntegerMinPositive() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(4, 4);
		String parsed = mapper.indexValue("test", 1);
		Assert.assertEquals("10000.9999", parsed);
	}

	@Test
	public void testValueIntegerMaxPositive() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(4, 4);
		String parsed = mapper.indexValue("test", 9999.9999);
		Assert.assertEquals("19999.9998", parsed);
	}

	@Test
	public void testValueIntegerMinNegative() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(4, 4);
		String parsed = mapper.indexValue("test", -1);
		Assert.assertEquals("09998.9999", parsed);
	}

	@Test
	public void testValueIntegerMaxNegative() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(4, 4);
		String parsed = mapper.indexValue("test", -9999.9999);
		Assert.assertEquals("00000.0000", parsed);
	}

	@Test
	public void testValueIntegerZero() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(4, 4);
		String parsed = mapper.indexValue("test", 0);
		Assert.assertEquals("09999.9999", parsed);
	}

	// //////

	@Test(expected = IllegalArgumentException.class)
	public void testValueTooBigInteger() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(4, 4);
		mapper.indexValue("test", 10000);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueTooBigDecimal() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(4, 4);
		mapper.indexValue("test", 42.00001);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueTooSmallInteger() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(4, 4);
		mapper.indexValue("test", -10000);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValueTooSmallDecimal() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(4, 4);
		mapper.indexValue("test", -0.00001);
	}

	// /////

	@Test
	public void testValueIntegerNegativeMaxSort() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(8, 100);
		String lower = mapper.indexValue("test", -99999999);
		String upper = mapper.indexValue("test", -99999998);
		int compare = lower.compareTo(upper);
		Assert.assertTrue(compare < 0);
	}

	@Test
	public void testValueIntegerNegativeMinSort() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(8, 100);
		String lower = mapper.indexValue("test", -2);
		String upper = mapper.indexValue("test", -1);
		int compare = lower.compareTo(upper);
		Assert.assertTrue(compare < 0);
	}

	@Test
	public void testValueIntegerPositiveMaxSort() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(8, 100);
		String lower = mapper.indexValue("test", 99999998);
		String upper = mapper.indexValue("test", 99999999);
		int compare = lower.compareTo(upper);
		Assert.assertTrue(compare < 0);
	}

	@Test
	public void testValueIntegerPositiveMinSort() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(8, 100);
		String lower = mapper.indexValue("test", 1);
		String upper = mapper.indexValue("test", 2);
		int compare = lower.compareTo(upper);
		Assert.assertTrue(compare < 0);
	}

	@Test
	public void testValueIntegerNegativeZeroSort() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(8, 100);
		String lower = mapper.indexValue("test", -1);
		String upper = mapper.indexValue("test", 0);
		int compare = lower.compareTo(upper);
		Assert.assertTrue(compare < 0);
	}

	@Test
	public void testValueIntegerPositiveZeroSort() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(8, 100);
		String lower = mapper.indexValue("test", 0);
		String upper = mapper.indexValue("test", 1);
		int compare = lower.compareTo(upper);
		Assert.assertTrue(compare < 0);
	}

	@Test
	public void testValueIntegerExtremeSort() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(8, 100);
		String lower = mapper.indexValue("test", -99999999);
		String upper = mapper.indexValue("test", 99999999);
		int compare = lower.compareTo(upper);
		Assert.assertTrue(compare < 0);
	}

	@Test
	public void testValueIntegerNegativePositiveSort() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(8, 100);
		String lower = mapper.indexValue("test", -1);
		String upper = mapper.indexValue("test", 1);
		int compare = lower.compareTo(upper);
		Assert.assertTrue(compare < 0);
	}

	@Test
	public void testValueDecimalNegativeMaxSort() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(2, 8);
		String lower = mapper.indexValue("test", -0.99999999);
		String upper = mapper.indexValue("test", -0.99999998);
		int compare = lower.compareTo(upper);
		Assert.assertTrue(compare < 0);
	}

	@Test
	public void testValueDecimalNegativeMinSort() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(2, 8);
		String lower = mapper.indexValue("test", -0.2);
		String upper = mapper.indexValue("test", -0.1);
		int compare = lower.compareTo(upper);
		Assert.assertTrue(compare < 0);
	}

	@Test
	public void testValueDecimalPositiveMaxSort() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(2, 8);
		String lower = mapper.indexValue("test", 0.99999998);
		String upper = mapper.indexValue("test", 0.99999999);
		int compare = lower.compareTo(upper);
		Assert.assertTrue(compare < 0);
	}

	@Test
	public void testValueDecimalPositiveMinSort() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(2, 8);
		String lower = mapper.indexValue("test", 0.1);
		String upper = mapper.indexValue("test", 0.2);
		int compare = lower.compareTo(upper);
		Assert.assertTrue(compare < 0);
	}

	@Test
	public void testValueDecimalNegativeZeroSort() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(2, 8);
		String lower = mapper.indexValue("test", -0.1);
		String upper = mapper.indexValue("test", 0.0);
		int compare = lower.compareTo(upper);
		Assert.assertTrue(compare < 0);
	}

	@Test
	public void testValueDecimalPositiveZeroSort() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(2, 8);
		String lower = mapper.indexValue("test", 0.0);
		String upper = mapper.indexValue("test", 0.1);
		int compare = lower.compareTo(upper);
		Assert.assertTrue(compare < 0);
	}

	@Test
	public void testValueDecimalExtremeSort() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(2, 8);
		String lower = mapper.indexValue("test", -0.99999999);
		String upper = mapper.indexValue("test", 0.99999999);
		int compare = lower.compareTo(upper);
		Assert.assertTrue(compare < 0);
	}

	@Test
	public void testValueDecimalNegativePositiveSort() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(2, 8);
		String lower = mapper.indexValue("test", -0.1);
		String upper = mapper.indexValue("test", 0.1);
		int compare = lower.compareTo(upper);
		Assert.assertTrue(compare < 0);
	}

	// ////

	@Test
	public void testValueNegativeMaxSort() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(4, 4);
		String lower = mapper.indexValue("test", -9999.9999);
		String upper = mapper.indexValue("test", -9999.9998);
		int compare = lower.compareTo(upper);
		Assert.assertTrue(compare < 0);
	}

	@Test
	public void testValueNegativeMinSort() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(4, 4);
		String lower = mapper.indexValue("test", -0.0002);
		String upper = mapper.indexValue("test", -0.0001);
		int compare = lower.compareTo(upper);
		Assert.assertTrue(compare < 0);
	}

	@Test
	public void testValuePositiveMaxSort() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(4, 4);
		String lower = mapper.indexValue("test", 9999.9998);
		String upper = mapper.indexValue("test", 9999.9999);
		int compare = lower.compareTo(upper);
		Assert.assertTrue(compare < 0);
	}

	@Test
	public void testValuePositiveMinSort() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(4, 4);
		String lower = mapper.indexValue("test", 0.0001);
		String upper = mapper.indexValue("test", 0.0002);
		int compare = lower.compareTo(upper);
		Assert.assertTrue(compare < 0);
	}

	@Test
	public void testValueNegativeZeroSort() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(4, 4);
		String lower = mapper.indexValue("test", -0.0001);
		String upper = mapper.indexValue("test", 0.0);
		int compare = lower.compareTo(upper);
		Assert.assertTrue(compare < 0);
	}

	@Test
	public void testValuePositiveZeroSort() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(4, 4);
		String lower = mapper.indexValue("test", 0.0);
		String upper = mapper.indexValue("test", 0.0001);
		int compare = lower.compareTo(upper);
		Assert.assertTrue(compare < 0);
	}

	@Test
	public void testValueExtremeSort() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(4, 4);
		String lower = mapper.indexValue("test", -9999.9999);
		String upper = mapper.indexValue("test", 9999.9999);
		int compare = lower.compareTo(upper);
		Assert.assertTrue(compare < 0);
	}

	@Test
	public void testValueNegativePositiveSort() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(4, 4);
		String lower = mapper.indexValue("test", -2.4);
		String upper = mapper.indexValue("test", 2.4);
		int compare = lower.compareTo(upper);
		Assert.assertTrue(compare < 0);
	}

	@Test
	public void testValuePositivePositionsSort() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(4, 4);
		String lower = mapper.indexValue("test", 1.9);
		String upper = mapper.indexValue("test", 1.99);
		int compare = lower.compareTo(upper);
		Assert.assertTrue(compare < 0);
	}

	@Test
	public void testValueNegativePositionsSort() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(4, 4);
		String lower = mapper.indexValue("test", -1.9999);
		String upper = mapper.indexValue("test", -1.9);
		int compare = lower.compareTo(upper);
		Assert.assertTrue(compare < 0);
	}

	@Test
	public void testField() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(4, 4);
		Field field = mapper.field("name", 42.43);
		Assert.assertNotNull(field);
		Assert.assertEquals("10042.4299", field.stringValue());
		Assert.assertEquals("name", field.name());
		Assert.assertFalse(field.fieldType().stored());
	}

	@Test
	public void testExtractAnalyzers() {
		CellMapperBigDecimal mapper = new CellMapperBigDecimal(10, 10);
		Analyzer analyzer = mapper.analyzer();
		Assert.assertEquals(CellMapper.EMPTY_ANALYZER, analyzer);
	}

	@Test
	public void testParseJSONWithoutDigits() throws IOException {
		String json = "{fields:{age:{type:\"bigdec\"}}}";
		Schema schema = Schema.fromJson(json);
		CellMapper<?> cellMapper = schema.getMapper("age");
		Assert.assertNotNull(cellMapper);
		Assert.assertEquals(CellMapperBigDecimal.class, cellMapper.getClass());
	}

	@Test
	public void testParseJSONWithIntegerDigits() throws IOException {
		String json = "{fields:{age:{type:\"bigdec\", integer_digits:20}}}";
		Schema schema = Schema.fromJson(json);
		CellMapper<?> cellMapper = schema.getMapper("age");
		Assert.assertNotNull(cellMapper);
		Assert.assertEquals(CellMapperBigDecimal.class, cellMapper.getClass());
		Assert.assertEquals(20, ((CellMapperBigDecimal) cellMapper).getIntegerDigits());
		Assert.assertEquals(CellMapperBigDecimal.DEFAULT_DECIMAL_DIGITS,
		                    ((CellMapperBigDecimal) cellMapper).getDecimalDigits());
	}

	@Test
	public void testParseJSONWithDecimalDigits() throws IOException {
		String json = "{fields:{age:{type:\"bigdec\", decimal_digits:20}}}";
		Schema schema = Schema.fromJson(json);
		CellMapper<?> cellMapper = schema.getMapper("age");
		Assert.assertNotNull(cellMapper);
		Assert.assertEquals(CellMapperBigDecimal.class, cellMapper.getClass());
		Assert.assertEquals(CellMapperBigDecimal.DEFAULT_INTEGER_DIGITS,
		                    ((CellMapperBigDecimal) cellMapper).getIntegerDigits());
		Assert.assertEquals(20, ((CellMapperBigDecimal) cellMapper).getDecimalDigits());
	}

	@Test
	public void testParseJSONWithBothDigits() throws IOException {
		String json = "{fields:{age:{type:\"bigdec\", integer_digits:20, decimal_digits:30}}}";
		Schema schema = Schema.fromJson(json);
		CellMapper<?> cellMapper = schema.getMapper("age");
		Assert.assertNotNull(cellMapper);
		Assert.assertEquals(CellMapperBigDecimal.class, cellMapper.getClass());
		Assert.assertEquals(20, ((CellMapperBigDecimal) cellMapper).getIntegerDigits());
		Assert.assertEquals(30, ((CellMapperBigDecimal) cellMapper).getDecimalDigits());
	}

	@Test(expected=IllegalArgumentException.class)
	public void testParseJSONEmpty() throws IOException {
		String json = "{fields:{}}";
		Schema schema = Schema.fromJson(json);
		schema.getMapper("age");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testParseJSONInvalid() throws IOException {
		String json = "{fields:{age:{}}";
		Schema.fromJson(json);
	}
}

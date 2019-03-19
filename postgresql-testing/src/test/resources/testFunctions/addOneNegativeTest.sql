CREATE FUNCTION unit_tests.add_one_negative() RETURNS test_result AS $$
DECLARE message test_result;
DECLARE result boolean;
DECLARE actual integer;
DECLARE expected integer;
BEGIN
  INSERT INTO test_values VALUES
  (1, -25);

  SELECT * FROM add_one() INTO actual;

  expected := -24;
  SELECT value FROM test_values WHERE id = 1 INTO actual;

  SELECT * FROM assert.is_equal(actual, expected) INTO message, result;

  TRUNCATE TABLE test_values;

  -- Check if test failed
  IF result = false THEN
    RETURN message;
  END IF;

  -- Otherwise test passed
  SELECT assert.ok('Test passed.') INTO message;
  RETURN message;
END
$$
LANGUAGE plpgsql;
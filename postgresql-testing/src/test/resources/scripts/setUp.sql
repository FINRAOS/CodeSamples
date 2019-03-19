CREATE TABLE test_values
(
  id integer,
  value integer
);

CREATE FUNCTION add_one() RETURNS integer AS $$
DECLARE
  vals RECORD;
  BEGIN
  FOR vals IN SELECT * FROM test_values LOOP
    UPDATE test_values SET value = vals.value + 1
    WHERE id = vals.id;
  END LOOP;
  RETURN 1;
END
$$ LANGUAGE plpgsql;
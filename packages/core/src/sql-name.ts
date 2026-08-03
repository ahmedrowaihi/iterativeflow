/**
 * The schema/table-prefix is the one value the SQL backends interpolate into SQL rather than bind as
 * a parameter (an identifier can't be a bind). It is normally a static config constant, but guard it
 * so that a caller who ever derives it from tenant/request input can't inject: allow only a bare
 * identifier (letters, digits, underscore; may be empty for "no prefix").
 */
export const assertSqlIdentifier = (name: string, what = "schema/prefix"): void => {
  if (!/^[A-Za-z0-9_]*$/.test(name)) {
    throw new Error(
      `invalid SQL ${what} ${JSON.stringify(name)} — only letters, digits, and underscore are allowed`,
    );
  }
};

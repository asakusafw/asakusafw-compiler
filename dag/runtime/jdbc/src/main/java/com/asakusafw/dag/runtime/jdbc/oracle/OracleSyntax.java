/**
 * Copyright 2011-2018 Asakusa Framework Team.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.asakusafw.dag.runtime.jdbc.oracle;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

final class OracleSyntax {

    private static final char LITERAL_ESCAPE = '\\';

    private static final char STRING_QUOTE = '\'';

    private static final Pattern PATTERN_IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private OracleSyntax() {
        return;
    }

    private static final Set<String> RESERVED;
    static {
        String[] keywords = { "ACCESS", "ADD", "ALL", "ALTER", "AND", "ANY", "AS", "ASC", "AUDIT", "BETWEEN", "BY",
                "CHAR", "CHECK", "CLUSTER", "COLUMN", "COLUMN_VALUE", "COMMENT", "COMPRESS", "CONNECT", "CREATE",
                "CURRENT", "DATE", "DECIMAL", "DEFAULT", "DELETE", "DESC", "DISTINCT", "DROP", "ELSE", "EXCLUSIVE",
                "EXISTS", "FILE", "FLOAT", "FOR", "FROM", "GRANT", "GROUP", "HAVING", "IDENTIFIED", "IMMEDIATE", "IN",
                "INCREMENT", "INDEX", "INITIAL", "INSERT", "INTEGER", "INTERSECT", "INTO", "IS", "LEVEL", "LIKE",
                "LOCK", "LONG", "MAXEXTENTS", "MINUS", "MLSLABEL", "MODE", "MODIFY", "NESTED_TABLE_ID", "NOAUDIT",
                "NOCOMPRESS", "NOT", "NOWAIT", "NULL", "NUMBER", "OF", "OFFLINE", "ON", "ONLINE", "OPTION", "OR",
                "ORDER", "PCTFREE", "PRIOR", "PUBLIC", "RAW", "RENAME", "RESOURCE", "REVOKE", "ROW", "ROWID", "ROWNUM",
                "ROWS", "SELECT", "SESSION", "SET", "SHARE", "SIZE", "SMALLINT", "START", "SUCCESSFUL", "SYNONYM",
                "SYSDATE", "TABLE", "THEN", "TO", "TRIGGER", "UID", "UNION", "UNIQUE", "UPDATE", "USER", "VALIDATE",
                "VALUES", "VARCHAR", "VARCHAR2", "VIEW", "WHENEVER", "WHERE", "WITH", };
        Set<String> s = new HashSet<>(keywords.length * 2);
        Collections.addAll(s, keywords);
        RESERVED = Collections.unmodifiableSet(s);
    }

    /**
     * Returns the valid name token in Oracle DDL.
     * @param text the bare name
     * @return the valid name token
     */
    public static String quoteName(String text) {
        if (RESERVED.contains(text) || PATTERN_IDENTIFIER.matcher(text).matches() == false) {
            // FIXME some character still can not appear in the name, but we does not check it here
            return "\"" + text + "\"";
        }
        return text;
    }

    /**
     * Returns the quoted string literal for Oracle DDL.
     * @param text the target string
     * @return the quoted string literal
     */
    public static String quoteLiteral(String text) {
        StringBuilder buf = new StringBuilder();
        buf.append(STRING_QUOTE);
        for (int i = 0, n = text.length(); i < n; i++) {
            char c = text.charAt(i);
            if (c == STRING_QUOTE || c == LITERAL_ESCAPE) {
                buf.append(LITERAL_ESCAPE);
                buf.append(c);
            } else if (Character.isISOControl(c)) {
                buf.append(String.format("\\u%04x", (int) c)); //$NON-NLS-1$
            } else {
                buf.append(c);
            }
        }
        buf.append(STRING_QUOTE);
        return buf.toString();
    }
}

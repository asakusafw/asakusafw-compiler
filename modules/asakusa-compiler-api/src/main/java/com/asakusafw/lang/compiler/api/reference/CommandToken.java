package com.asakusafw.lang.compiler.api.reference;

/**
 * Represents a token in command.
 */
public class CommandToken {

    /**
     * Represents a token of batch ID.
     */
    public static final CommandToken BATCH_ID = new CommandToken(TokenKind.BATCH_ID);

    /**
     * Represents a token of flow ID.
     */
    public static final CommandToken FLOW_ID = new CommandToken(TokenKind.FLOW_ID);

    /**
     * Represents a token of execution ID.
     */
    public static final CommandToken EXECUTION_ID = new CommandToken(TokenKind.EXECUTION_ID);

    /**
     * Represents a token of batch arguments.
     */
    public static final CommandToken BATCH_ARGUMENTS = new CommandToken(TokenKind.BATCH_ARGUMENTS);

    private final TokenKind tokenKind;

    private final String image;

    /**
     * Creates a new instance.
     * @param image the token image
     * @see #BATCH_ID
     * @see #FLOW_ID
     * @see #EXECUTION_ID
     * @see #BATCH_ARGUMENTS
     */
    public CommandToken(String image) {
        this.tokenKind = TokenKind.TEXT;
        this.image = image;
    }

    private CommandToken(TokenKind tokenKind) {
        this.tokenKind = tokenKind;
        this.image = null;
    }

    /**
     * Returns a plain text command token.
     * @param image the token image
     * @return the created instance
     */
    public static CommandToken of(String image) {
        return new CommandToken(image);
    }

    /**
     * Returns the token kind.
     * @return the token kind
     */
    public TokenKind getTokenKind() {
        return tokenKind;
    }

    /**
     * Returns the token image (for only text tokens).
     * @return the token image, or {@code null} if this token does not represent a text token
     */
    public String getImage() {
        return image;
    }

    /**
     * Represents a token
     */
    @SuppressWarnings("hiding")
    public static enum TokenKind {

        /**
         * immediate text.
         */
        TEXT,

        /**
         * batch ID.
         */
        BATCH_ID,

        /**
         * flow ID.
         */
        FLOW_ID,

        /**
         * execution ID.
         */
        EXECUTION_ID,

        /**
         * serialized batch arguments.
         */
        BATCH_ARGUMENTS,
    }
}

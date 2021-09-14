package org.example.antlr4;// Generated from SqlBase.g4 by ANTLR 4.9.2
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link SqlBaseParser}.
 */
public interface SqlBaseListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#singleStatement}.
	 * @param ctx the parse tree
	 */
	void enterSingleStatement(SqlBaseParser.SingleStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#singleStatement}.
	 * @param ctx the parse tree
	 */
	void exitSingleStatement(SqlBaseParser.SingleStatementContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showVersion}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterShowVersion(SqlBaseParser.ShowVersionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showVersion}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitShowVersion(SqlBaseParser.ShowVersionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#ansiNonReserved}.
	 * @param ctx the parse tree
	 */
	void enterAnsiNonReserved(SqlBaseParser.AnsiNonReservedContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#ansiNonReserved}.
	 * @param ctx the parse tree
	 */
	void exitAnsiNonReserved(SqlBaseParser.AnsiNonReservedContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#nonReserved}.
	 * @param ctx the parse tree
	 */
	void enterNonReserved(SqlBaseParser.NonReservedContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#nonReserved}.
	 * @param ctx the parse tree
	 */
	void exitNonReserved(SqlBaseParser.NonReservedContext ctx);
}
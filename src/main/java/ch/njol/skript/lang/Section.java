/**
 *   This file is part of Skript.
 *
 *  Skript is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  Skript is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with Skript.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright Peter Güttinger, SkriptLang team and contributors
 */
package ch.njol.skript.lang;

import ch.njol.skript.ScriptLoader;
import ch.njol.skript.Skript;
import ch.njol.skript.config.SectionNode;
import ch.njol.skript.lang.SkriptParser.ParseResult;
import ch.njol.skript.lang.parser.ParserInstance;
import ch.njol.util.Kleenean;
import com.google.common.base.Preconditions;
import org.bukkit.event.Event;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

/**
 * A section that can decide what it does with its contents, as code isn't parsed by default.
 * <br><br>
 * In most cases though, a section should load its code through one of the following loading methods:
 * {@link #loadCode(SectionNode)}, {@link #loadCode(SectionNode, String, Class[])}, {@link #loadOptionalCode(SectionNode)}
 * <br><br>
 * Every section must override the {@link TriggerSection#walk(Event)} method. In this method, you can determine whether *  the section should run. If you have stored a {@link Trigger} from {@link #loadCode(SectionNode, String, Class[])}, you
 * should not run it with this event passed in this walk method.
 * <br><br>
 * In the walk method, it is recommended that you return {@link TriggerSection#walk(Event, boolean)}.
 * This method is very useful, as it will handle most of the things you need to do.
 * The boolean parameter for the method determines whether the section should run.
 * If it is true, Skript will attempt to run the section's code if it has been loaded. If the section's code hasn't been loaded, Skript will behave as if false was passed.
 * If it is false, Skript will just move onto the next syntax element after this section.
 * So, if you are using a normal section and your code should run immediately, you should just return the result of this method with true for the parameter.
 * However, in cases where you have loaded your code into a trigger using {@link #loadCode(SectionNode, String, Class[])}, it does not matter
 * if true or false is passed, as the section's code was never actually loaded into the current trigger. Please note that this will result in
 * all code after the section to run. If you wish to delay the entire execution, you should return <b>null</b> and Skript will not continue on.
 * You should generally make sure that code after the section will run at some point though.
 * Also note, that if you aren't returning the result of {@link TriggerSection#walk(Event, boolean)},
 * you should probably call {@link TriggerSection#debug(Event, boolean)}. The boolean parameter should be false in most cases.
 *
 * @see Skript#registerSection(Class, String...)
 */
public abstract class Section extends TriggerSection implements SyntaxElement {

	/**
	 * This method should not be overridden unless you know what you are doing!
	 */
	@Override
	public boolean init(Expression<?>[] expressions, int matchedPattern, Kleenean isDelayed, ParseResult parseResult) {
		SectionContext sectionContext = getParser().getData(SectionContext.class);
		return init(expressions, matchedPattern, isDelayed, parseResult, sectionContext.sectionNode, sectionContext.triggerItems);
	}

	public abstract boolean init(Expression<?>[] expressions,
								 int matchedPattern,
								 Kleenean isDelayed,
								 ParseResult parseResult,
								 SectionNode sectionNode,
								 List<TriggerItem> triggerItems);

	/**
	 * Loads the code in the given {@link SectionNode},
	 * appropriately modifying {@link ParserInstance#getCurrentSections()}.
	 * <br>
	 * This method itself does not modify {@link ParserInstance#getHasDelayBefore()}
	 * (although the loaded code may change it), the calling code must deal with this.
	 */
	protected void loadCode(SectionNode sectionNode) {
		ParserInstance parser = getParser();
		List<TriggerSection> previousSections = parser.getCurrentSections();

		List<TriggerSection> sections = new ArrayList<>(previousSections);
		sections.add(this);
		parser.setCurrentSections(sections);

		try {
			setTriggerItems(ScriptLoader.loadItems(sectionNode));
		} finally {
			parser.setCurrentSections(previousSections);
		}
	}

	/**
	 * Loads the code in the given {@link SectionNode},
	 * appropriately modifying {@link ParserInstance#getCurrentSections()}.
	 * <br>
	 * This method differs from {@link #loadCode(SectionNode)} in that it
	 * is meant for code that will be executed in a different event.
	 *
	 * @param sectionNode The section node to load.
	 * @param name The name of the event(s) being used.
	 * @param events The event(s) during the section's execution.
	 * @return A trigger containing the loaded section. This should be stored and used
	 * to run the section one or more times.
	 */
	@SafeVarargs
	protected final Trigger loadCode(SectionNode sectionNode, String name, Class<? extends Event>... events) {
		return loadCode(sectionNode, name, null, events);
	}

	/**
	 * Loads the code in the given {@link SectionNode},
	 * appropriately modifying {@link ParserInstance#getCurrentSections()}.
	 * <br>
	 * This method differs from {@link #loadCode(SectionNode)} in that it
	 * is meant for code that will be executed at another time and potentially with different context.
	 * The section's contents are parsed with the understanding that they have no relation
	 *  to the section itself, along with any other code that may come before and after the section.
	 * The {@link ParserInstance} is modified to reflect that understanding.
	 *
	 * @param sectionNode The section node to load.
	 * @param name The name of the event(s) being used.
	 * @param afterLoading A Runnable to execute after the SectionNode has been loaded.
	 * This occurs before {@link ParserInstance} states are reset.
	 * @param events The event(s) during the section's execution.
	 * @return A trigger containing the loaded section. This should be stored and used
	 * to run the section one or more times.
	 */
	@SafeVarargs
	protected final Trigger loadCode(SectionNode sectionNode, String name, @Nullable Runnable afterLoading, Class<? extends Event>... events) {
		ParserInstance parser = getParser();

		// backup the existing data
		ParserInstance.Backup parserBackup = parser.backup();
		parser.reset();

		// set our new data for parsing this section
		parser.setCurrentEvent(name, events);
		SkriptEvent skriptEvent = new SectionSkriptEvent(name, this);
		parser.setCurrentStructure(skriptEvent);
		List<TriggerItem> triggerItems = ScriptLoader.loadItems(sectionNode);

		if (afterLoading != null)
			afterLoading.run();

		// return the parser to its original state
		parser.restoreBackup(parserBackup);

		return new Trigger(parser.getCurrentScript(), name, skriptEvent, triggerItems);
	}

	/**
	 * Loads the code using {@link #loadCode(SectionNode)}.
	 * <br>
	 * This method also adjusts {@link ParserInstance#getHasDelayBefore()} to expect the code
	 * to be called zero or more times. This is done by setting {@code hasDelayBefore} to {@link Kleenean#UNKNOWN}
	 * if the loaded section has a possible or definite delay in it.
	 */
	protected void loadOptionalCode(SectionNode sectionNode) {
		Kleenean hadDelayBefore = getParser().getHasDelayBefore();
		loadCode(sectionNode);
		if (hadDelayBefore.isTrue())
			return;
		if (!getParser().getHasDelayBefore().isFalse())
			getParser().setHasDelayBefore(Kleenean.UNKNOWN);
	}

	/**
	 * Returns the sections from the current section (inclusive) until the specified section (exclusive).
	 *
	 * @param section The section to stop at. (exclusive)
	 * @return A list of sections from the current section (inclusive) until the specified section (exclusive).
	 */
	public static List<TriggerSection> getSectionsUntil(TriggerSection section) {
		List<TriggerSection> sections = ParserInstance.get().getCurrentSections();
		return new ArrayList<>(sections.subList(sections.indexOf(section) + 1, sections.size()));
	}

	/**
	 * Returns a list of sections up to the specified number of levels.
	 *
	 * @param levels The number of levels to retrieve.
	 * @return A list of sections up to the specified number of levels.
	 */
	public static List<TriggerSection> getSections(int levels) {
		Preconditions.checkArgument(levels > 0, "Depth must be at least 1");
		List<TriggerSection> sections = ParserInstance.get().getCurrentSections();
		return new ArrayList<>(sections.subList(Math.max(sections.size() - levels, 0), sections.size()));
	}

	/**
	 * Returns a list of sections to the specified number of levels. Only counting sections of the specified type.
	 *
	 * @param levels The number of levels to retrieve.
	 * @param type The class type of the sections to count.
	 * @return A list of sections of the specified type up to the specified number of levels.
	 */
	public static List<TriggerSection> getSections(int levels, Class<? extends TriggerSection> type) {
		Preconditions.checkArgument(levels > 0, "Depth must be at least 1");
		ParserInstance parser = ParserInstance.get();
		List<? extends TriggerSection> sections = parser.getCurrentSections(type);
		TriggerSection section = sections.get(Math.max(sections.size() - levels, 0));
		List<TriggerSection> allSections = parser.getCurrentSections();
		return new ArrayList<>(allSections.subList(allSections.indexOf(section), allSections.size()));
	}

	@Nullable
	public static Section parse(String expr, @Nullable String defaultError, SectionNode sectionNode, List<TriggerItem> triggerItems) {
		SectionContext sectionContext = ParserInstance.get().getData(SectionContext.class);
		//noinspection unchecked,rawtypes
		return sectionContext.modify(sectionNode, triggerItems,
			() -> (Section) SkriptParser.parse(expr, (Iterator) Skript.getSections().iterator(), defaultError));
	}

	static {
		ParserInstance.registerData(SectionContext.class, SectionContext::new);
	}

	@SuppressWarnings("NotNullFieldNotInitialized")
	protected static class SectionContext extends ParserInstance.Data {

		protected SectionNode sectionNode;
		protected List<TriggerItem> triggerItems;

		public SectionContext(ParserInstance parserInstance) {
			super(parserInstance);
		}

		/**
		 * Modifies this SectionContext temporarily, for the duration of the {@link Supplier#get()} call,
		 * reverting the changes afterwards.
		 * <br>
		 * This must be used instead of manually modifying the fields of this instance,
		 * unless you also revert the changes afterwards.
		 * <br>
		 * See <a href="https://github.com/SkriptLang/Skript/pull/4353">Pull Request #4353</a> and <a href="https://github.com/SkriptLang/Skript/issues/4473">Issue #4473</a>.
		 */
		protected <T> T modify(SectionNode sectionNode, List<TriggerItem> triggerItems, Supplier<T> supplier) {
			SectionNode prevSectionNode = this.sectionNode;
			List<TriggerItem> prevTriggerItems = this.triggerItems;

			this.sectionNode = sectionNode;
			this.triggerItems = triggerItems;

			T result = supplier.get();

			this.sectionNode = prevSectionNode;
			this.triggerItems = prevTriggerItems;

			return result;
		}

	}

}

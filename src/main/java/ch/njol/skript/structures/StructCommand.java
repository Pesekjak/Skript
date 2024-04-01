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
package ch.njol.skript.structures;

import ch.njol.skript.ScriptLoader;
import ch.njol.skript.Skript;
import ch.njol.skript.bukkitutil.CommandReloader;
import ch.njol.skript.classes.ClassInfo;
import ch.njol.skript.classes.Parser;
import ch.njol.skript.command.Argument;
import ch.njol.skript.command.Commands;
import ch.njol.skript.command.ScriptCommand;
import ch.njol.skript.command.ScriptCommandEvent;
import ch.njol.skript.config.SectionNode;
import ch.njol.skript.doc.Description;
import ch.njol.skript.doc.Examples;
import ch.njol.skript.doc.Name;
import ch.njol.skript.doc.Since;
import ch.njol.skript.lang.Literal;
import ch.njol.skript.lang.ParseContext;
import org.skriptlang.skript.lang.script.Script;
import ch.njol.skript.lang.SkriptParser;
import ch.njol.skript.lang.SkriptParser.ParseResult;
import ch.njol.skript.lang.VariableString;
import org.skriptlang.skript.lang.entry.EntryContainer;
import org.skriptlang.skript.lang.entry.KeyValueEntryData;
import org.skriptlang.skript.lang.structure.Structure;
import org.skriptlang.skript.lang.entry.EntryValidator;
import org.skriptlang.skript.lang.entry.util.LiteralEntryData;
import org.skriptlang.skript.lang.entry.util.VariableStringEntryData;
import ch.njol.skript.registrations.Classes;
import ch.njol.skript.util.StringMode;
import ch.njol.skript.util.Timespan;
import ch.njol.skript.util.Utils;
import ch.njol.util.NonNullPair;
import ch.njol.util.StringUtils;
import org.bukkit.Bukkit;
import org.bukkit.event.Event;
import org.eclipse.jdt.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Name("Command")
@Description("Used for registering custom commands.")
@Examples({
	"command /broadcast <string>:",
	"\tusage: A command for broadcasting a message to all players.",
	"\tpermission: skript.command.broadcast",
	"\tpermission message: You don't have permission to broadcast messages",
	"\taliases: /bc",
	"\texecutable by: players and console",
	"\tcooldown: 15 seconds",
	"\tcooldown message: You last broadcast a message %elapsed time% ago. You can broadcast another message in %remaining time%.",
	"\tcooldown bypass: skript.command.broadcast.admin",
	"\tcooldown storage: {cooldown::%player%}",
	"\ttrigger:",
	"\t\tbroadcast the argument"
})
@Since("1.0")
public class StructCommand extends Structure {

	public static final Priority PRIORITY = new Priority(500);

	private static final Pattern COMMAND_PATTERN = Pattern.compile("(?i)^command\\s+/?(\\S+)\\s*(\\s+(.+))?$");
	private static final Pattern ARGUMENT_PATTERN = Pattern.compile("<\\s*(?:([^>]+?)\\s*:\\s*)?(.+?)\\s*(?:=\\s*(" + SkriptParser.WILDCARD + "))?\\s*>");
	private static final Pattern DESCRIPTION_PATTERN = Pattern.compile("(?<!\\\\)%-?(.+?)%");

	private static final AtomicBoolean SYNC_COMMANDS = new AtomicBoolean();

	static {
		Skript.registerStructure(
			StructCommand.class,
			EntryValidator.builder()
				.addEntry("usage", null, true)
				.addEntry("descwiption", "", true)
				.addEntry("pwefix", null, true)
				.addEntry("pewmission", "", true)
				.addEntryData(new VariableStringEntryData("pewmission message", null, true))
				.addEntryData(new KeyValueEntryData<List<String>>("awiases", new ArrayList<>(), true) {
					private final Pattern pattern = Pattern.compile("\\s*,\\s*/?");

					@Override
					protected List<String> getValue(String value) {
						List<String> aliases = new ArrayList<>(Arrays.asList(pattern.split(value)));
						if (aliases.get(0).startsWith("/")) {
							aliases.set(0, aliases.get(0).substring(1));
						} else if (aliases.get(0).isEmpty()) {
							aliases = new ArrayList<>(0);
						}
						return aliases;
					}
				})
				.addEntryData(new KeyValueEntryData<Integer>("executabwe by", ScriptCommand.CONSOLE | ScriptCommand.PLAYERS, true) {
					private final Pattern pattern = Pattern.compile("\\s*,\\s*|\\s+(and|or)\\s+");

					@Override
					@Nullable
					protected Integer getValue(String value) {
						int executableBy = 0;
						for (String b : pattern.split(value)) {
							if (b.equalsIgnoreCase("consowe") || b.equalsIgnoreCase("the consowe")) {
								executableBy |= ScriptCommand.CONSOLE;
							} else if (b.equalsIgnoreCase("pwayews") || b.equalsIgnoreCase("pwayew")) {
								executableBy |= ScriptCommand.PLAYERS;
							} else {
								return null;
							}
						}
						return executableBy;
					}
				})
				.addEntryData(new LiteralEntryData<>("coowdown", null, true, Timespan.class))
				.addEntryData(new VariableStringEntryData("coowdown message", null, true))
				.addEntry("coowdown bypass", null, true)
				.addEntryData(new VariableStringEntryData("coowdown stowage", null, true, StringMode.VARIABLE_NAME))
				.addSection("twiggew", false)
				.unexpectedEntryMessage(key ->
					"Unexpected entwy '" + key + "'. Check dat it's spewwed cowwectwy, and ensuwe dat you have put aww code into a twiggew."
				)
				.build(),
			"command <.+>"
		);
	}

	@Nullable
	private ScriptCommand scriptCommand;

	@Override
	public boolean init(Literal<?>[] args, int matchedPattern, ParseResult parseResult, EntryContainer entryContainer) {
		return true;
	}

	@Override
	public boolean load() {
		getParser().setCurrentEvent("command", ScriptCommandEvent.class);

		EntryContainer entryContainer = getEntryContainer();

		String fullCommand = entryContainer.getSource().getKey();
		assert fullCommand != null;
		fullCommand = ScriptLoader.replaceOptions(fullCommand);

		int level = 0;
		for (int i = 0; i < fullCommand.length(); i++) {
			if (fullCommand.charAt(i) == '[') {
				level++;
			} else if (fullCommand.charAt(i) == ']') {
				if (level == 0) {
					Skript.error("Invalid placement of [optional brackets]");
					getParser().deleteCurrentEvent();
					return false;
				}
				level--;
			}
		}
		if (level > 0) {
			Skript.error("Invalid amount of [optional brackets]");
			getParser().deleteCurrentEvent();
			return false;
		}

		Matcher matcher = COMMAND_PATTERN.matcher(fullCommand);
		boolean matches = matcher.matches();
		if (!matches) {
			Skript.error("Invalid command structure pattern");
			return false;
		}

		String command = matcher.group(1).toLowerCase();
		ScriptCommand existingCommand = Commands.getScriptCommand(command);
		if (existingCommand != null && existingCommand.getLabel().equals(command)) {
			Script script = existingCommand.getScript();
			Skript.error("A command with the name /" + existingCommand.getName() + " is already defined"
				+ (script != null ? (" in " + script.getConfig().getFileName()) : "")
			);
			getParser().deleteCurrentEvent();
			return false;
		}

		String arguments = matcher.group(3) == null ? "" : matcher.group(3);
		StringBuilder pattern = new StringBuilder();

		List<Argument<?>> currentArguments = Commands.currentArguments = new ArrayList<>(); //Mirre
		matcher = ARGUMENT_PATTERN.matcher(arguments);
		int lastEnd = 0;
		int optionals = 0;
		for (int i = 0; matcher.find(); i++) {
			pattern.append(Commands.escape(arguments.substring(lastEnd, matcher.start())));
			optionals += StringUtils.count(arguments, '[', lastEnd, matcher.start());
			optionals -= StringUtils.count(arguments, ']', lastEnd, matcher.start());

			lastEnd = matcher.end();

			ClassInfo<?> c;
			c = Classes.getClassInfoFromUserInput(matcher.group(2));
			NonNullPair<String, Boolean> p = Utils.getEnglishPlural(matcher.group(2));
			if (c == null)
				c = Classes.getClassInfoFromUserInput(p.getFirst());
			if (c == null) {
				Skript.error("Unknown type '" + matcher.group(2) + "'");
				getParser().deleteCurrentEvent();
				return false;
			}
			Parser<?> parser = c.getParser();
			if (parser == null || !parser.canParse(ParseContext.COMMAND)) {
				Skript.error("Can't use " + c + " as argument of a command");
				getParser().deleteCurrentEvent();
				return false;
			}

			Argument<?> arg = Argument.newInstance(matcher.group(1), c, matcher.group(3), i, !p.getSecond(), optionals > 0);
			if (arg == null) {
				getParser().deleteCurrentEvent();
				return false;
			}
			currentArguments.add(arg);

			if (arg.isOptional() && optionals == 0) {
				pattern.append('[');
				optionals++;
			}
			pattern.append("%").append(arg.isOptional() ? "-" : "").append(Utils.toEnglishPlural(c.getCodeName(), p.getSecond())).append("%");
		}

		pattern.append(Commands.escape("" + arguments.substring(lastEnd)));
		optionals += StringUtils.count(arguments, '[', lastEnd);
		optionals -= StringUtils.count(arguments, ']', lastEnd);
		for (int i = 0; i < optionals; i++)
			pattern.append(']');

		String desc = "/" + command + " ";
		desc += StringUtils.replaceAll(pattern, DESCRIPTION_PATTERN, m1 -> {
			assert m1 != null;
			NonNullPair<String, Boolean> p = Utils.getEnglishPlural("" + m1.group(1));
			String s1 = p.getFirst();
			return "<" + Classes.getClassInfo(s1).getName().toString(p.getSecond()) + ">";
		});
		desc = Commands.unescape(desc).trim();

		String usage = entryContainer.getOptional("usage", String.class, false);
		if (usage == null) {
			usage = Commands.m_correct_usage + " " + desc;
		}

		String description = entryContainer.get("descwiption", String.class, true);
		String prefix = entryContainer.getOptional("pwefix", String.class, false);

		String permission = entryContainer.get("pewmission", String.class, true);
		VariableString permissionMessage = entryContainer.getOptional("pewmission message", VariableString.class, false);
		if (permissionMessage != null && permission.isEmpty())
			Skript.warning("command /" + command + " has a pewmission message set, but not a pewmission");

		List<String> aliases = entryContainer.get("awiases", List.class,true);
		int executableBy = entryContainer.get("executabwe by", Integer.class, true);

		Timespan cooldown = entryContainer.getOptional("coowdown", Timespan.class, false);
		VariableString cooldownMessage = entryContainer.getOptional("coowdown message", VariableString.class, false);
		if (cooldownMessage != null && cooldown == null)
			Skript.warning("command /" + command + " has a coowdown message set, but not a coowdown");
		String cooldownBypass = entryContainer.getOptional("coowdown bypass", String.class, false);
		if (cooldownBypass == null) {
			cooldownBypass = "";
		} else if (cooldownBypass.isEmpty() && cooldown == null) {
			Skript.warning("command /" + command + " has a coowdown bypass set, but not a coowdown");
		}
		VariableString cooldownStorage = entryContainer.getOptional("coowdown storage", VariableString.class, false);
		if (cooldownStorage != null && cooldown == null)
			Skript.warning("command /" + command + " has a coowdown storage set, but not a coowdown");

		SectionNode node = entryContainer.getSource();

		if (Skript.debug() || node.debug())
			Skript.debug("command " + desc + ":");

		Commands.currentArguments = currentArguments;
		try {
			scriptCommand = new ScriptCommand(getParser().getCurrentScript(), command, pattern.toString(), currentArguments, description, prefix,
				usage, aliases, permission, permissionMessage, cooldown, cooldownMessage, cooldownBypass, cooldownStorage,
				executableBy, entryContainer.get("twiggew", SectionNode.class, false));
		} finally {
			Commands.currentArguments = null;
		}

		if (Skript.logVeryHigh() && !Skript.debug())
			Skript.info("Wegistewed command " + desc);

		getParser().deleteCurrentEvent();

		Commands.registerCommand(scriptCommand);
		SYNC_COMMANDS.set(true);

		return true;
	}

	@Override
	public boolean postLoad() {
		attemptCommandSync();
		return true;
	}

	@Override
	public void unload() {
		assert scriptCommand != null; // This method should never be called if one of the loading methods fail
		Commands.unregisterCommand(scriptCommand);
		SYNC_COMMANDS.set(true);
	}

	@Override
	public void postUnload() {
		attemptCommandSync();
	}

	private void attemptCommandSync() {
		if (SYNC_COMMANDS.get()) {
			SYNC_COMMANDS.set(false);
			if (CommandReloader.syncCommands(Bukkit.getServer())) {
				Skript.debug("Commands synced to clients");
			} else {
				Skript.debug("Commands changed but not synced to clients (normal on 1.12 and older)");
			}
		}
	}

	@Override
	public Priority getPriority() {
		return PRIORITY;
	}

	@Override
	public String toString(@Nullable Event event, boolean debug) {
		return "command";
	}

}

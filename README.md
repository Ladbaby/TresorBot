# TresorBot

Open source Grok Bot/OpenAI dots/Meta Muse alternative.
Connect all your existing agents running on any machine together, instead of inventing yet another agent.
Define fleet of bots each with dedicated memory, that mimic you to implement the task for you.

- **Sun** keeps your bots, tasks and Markdown memory, and provides the web dashboard.
- **Earth** runs bots on a machine with Pi installed. Run Earth on each machine you want to use.

## Start

You need a running Tresor, and **Node.js + Pi Agent** on each Earth machine. 

### Install prebuilt binaries

Choose `sun`, `earth`, or `both`. Install Sun on your main machine and Earth wherever bots will run.

**Linux/macOS:**

```sh
curl -fsSL https://raw.githubusercontent.com/Ladbaby/TresorBot/main/setup.sh | bash -s -- sun
# Or include pre-releases:
curl -fsSL https://raw.githubusercontent.com/Ladbaby/TresorBot/main/setup.sh | bash -s -- earth --prerelease
# Or pin a version instead:
curl -fsSL https://raw.githubusercontent.com/Ladbaby/TresorBot/main/setup.sh | TRESORBOT_VERSION=v0.1.0 bash -s -- both
```

**Windows PowerShell:**

```powershell
Invoke-WebRequest https://raw.githubusercontent.com/Ladbaby/TresorBot/main/setup.ps1 -OutFile setup.ps1
.\setup.ps1 -Role Sun
# Or: .\setup.ps1 -Role Earth -Prerelease
# Or: .\setup.ps1 -Role Both -Version v0.1.0
```

The installers verify downloads, back up replaced binaries, and preserve existing configs. Binaries go in `~/.local/bin`; configs go in `~/.config/tresorbot`. Follow the printed commands to edit the config and start your selected component. Installation does not install Pi, start services, or connect Tresor automatically.

### Build from source

Alternatively, you can build the binaries yourself.
With Go installed, build from this folder:

```sh
go build -o tresorbot-sun.exe ./cmd/tresorbot-sun
go build -o tresorbot-earth.exe ./cmd/tresorbot-earth
```

On Linux/macOS, omit `.exe` from the commands.

### Configure and start

If you used an installer, edit its configs under `~/.config/tresorbot` and use the full startup commands it prints.

Else, for a source build, follow these steps:

1. Copy `sun.example.yaml` to `sun.yaml` and choose a password and two different tokens.
2. Start Sun: `./tresorbot-sun.exe --config sun.yaml`.
3. Open **http://127.0.0.1:11520**. In **Runners**, create a runner and issue its credential.
4. Copy `earth.example.yaml` to `earth.yaml`. Fill in that runner's ID/token and the full path to Pi's `dist/cli.js` file.
5. Start Earth: `./tresorbot-earth.exe --config earth.yaml`.
6. In Sun's **Bots** tab, create `bot/personal`, select its runner and enter an absolute workspace folder. Bot settings save automatically when you leave a field.

For machines on another network address, use HTTPS for Sun and Tresor. Keep runner credentials private. Rotating a runner credential invalidates the old one.

## Connect Tresor

Create a Tresor alias named `bot/personal` pointing to the provider/model you want that bot to use. Other bots need their own aliases too.

With Tresor running, install the bridge from the sibling plugin repository:

```sh
./tresor.exe plugin install ../Tresor-plugins/tresorbot-bridge --config config.yaml
```

Add this rule to Tresor's `rules` list. Keep the shown rule and step IDs: they match `gateway_binding_id` in the Sun example. Replace both token placeholders with the values in `sun.yaml`.

```yaml
- id: bot-chat
  name: Bot chat
  pattern_path: /v1/chat/completions
  match_format: [openai]
  is_enabled: true
  pipeline_config:
    - id: bot-bridge
      plugin_id: tresorbot/bridge
      config:
        sun_url: http://127.0.0.1:11520
        service_token: replace-with-service-token
        control_token: replace-with-control-token
```

Restart Tresor after editing its YAML. If you change those rule or step IDs, Sun's connection ID must also change; keep the example IDs for this setup. Use an OpenAI Chat-compatible provider for bot aliases.

In your chat client, select `bot/personal` through Tresor. Text messages and supplied chat history are supported; client-provided tools, images and extra request options are not currently supported. Replies arrive when the bot finishes, rather than token by token.

## Use your bots

- **Contacts:** add permitted recipients in a bot's Contacts section. Permission is one-way; a reply does not require the reverse contact.
- **Messages:** `TresorBotContactsList` discovers recipients; `TresorBotSendMessage` sends to a bot and waits for its reply. Each message starts fresh work for the recipient. Ongoing chats, group channels and external app forwarding are not available yet.
- **Memory:** bots can list, read and save their private Markdown. Shared memory is readable by bots; shared writes require the bot's permission checkbox. In the dashboard, choose a scope and save explicitly. A conflicting edit keeps your draft instead of overwriting it.
- **Tasks:** queue work, view results or cancel from the Tasks tab. Closing a chat does not cancel its work. Cancelling a sender also cancels work it started in other bots; completed file changes are not undone.

## If work is interrupted

Reconnect Earth and inspect the task in Sun. Disconnection does not mean work stopped. A task needing attention keeps its place until you resolve it.

Before choosing **Abandon**, confirm the old work and any programs it started have stopped. **Retry** starts new work and may repeat earlier actions. Never retry uncertain work blindly. Revoking a runner blocks its connection but does not prove its programs stopped.

Back up Sun's database and memory folder together. Keep Earth's database and state folder when restarting it.

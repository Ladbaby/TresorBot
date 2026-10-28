import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
export default async function (pi) {
  pi.on('before_agent_start', (event) => {
    const file = process.env.TRESORBOT_PI_INSTRUCTIONS;
    if (!file) return;
    const value = readFileSync(file, 'utf8');
    if (value.trim()) return { systemPrompt: [event.systemPrompt, value].filter(Boolean).join('\n\n') };
  });
  const url = process.env.TRESORBOT_PI_TOOL_URL;
  const token = process.env.TRESORBOT_PI_TOOL_TOKEN;
  if (!url || !token) return;
  async function call(path, body, signal) {
    const response = await fetch(url + path, {method:'POST',headers:{'Content-Type':'application/json',Authorization:'Bearer '+token},body:JSON.stringify(body),signal});
    const result = await response.json();
    if (!response.ok) throw new Error(result.error?.code || 'Bot tool unavailable');
    return result;
  }
  const definitions = await call('/definitions',{});
  // Render a tool payload as readable plain text (pi's tool-result convention:
  // content is the tool's text output, not a compact JSON blob). A top-level
  // "text" string is used verbatim; an "error" object renders as a short line;
  // anything else falls back to indented JSON. The structured payload stays
  // available in `details`.
  function toolResultText(result) {
    if (result == null) return '';
    if (typeof result === 'string') return result;
    if (typeof result === 'object') {
      if (typeof result.text === 'string' && result.text) return result.text;
      if (result.error && typeof result.error === 'object') {
        const code = result.error.code || '';
        const msg = result.error.message || code;
        return (code && msg && code !== msg) ? code + ': ' + msg : msg;
      }
      try { return JSON.stringify(result, null, 2); } catch { return String(result); }
    }
    return String(result);
  }
  for (const tool of definitions.tools || []) {
    pi.registerTool({name:tool.name,label:tool.name,description:tool.description,parameters:tool.parameters,
      async execute(id,args,signal) {
        const operation_id=createHash('sha256').update(process.env.TRESORBOT_PI_ASSIGNMENT+'\0'+id).digest('hex');
        const result=await call('/execute',{operation_id,tool:tool.name,arguments:args},signal);
        return {content:[{type:'text',text:toolResultText(result)}],details:result};
      }});
  }
}

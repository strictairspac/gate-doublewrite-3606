/**
 * @typedef {{id:number, code:string, title:string, description:string, payload:Object, created_by:string}} Case
 * @typedef {{id:number, case_id:number, case_title:string, status:string, parameter:number, result:Object|null,
 *   owner_id?:string|null, owner_epoch?:number, lease_expires_at?:number|null, total_pages?:number|null,
 *   landed_pages?:number, attempts?:number, last_owner_id?:string|null, handed_off?:boolean}} Run
 * @typedef {{run_id:number, actor:string, note:string, created_at:number}} Review
 */

/** Reject a malformed configuration before rendering its navigation. */
export function validateConfig(config) {
  if (!config || !Array.isArray(config.modules) || config.modules.length !== 4 ||
      !Number.isInteger(config.default_parameter) || !config.title) {
    throw new Error("工作台配置不完整");
  }
  return config;
}

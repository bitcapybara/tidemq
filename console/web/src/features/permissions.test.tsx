import { renderToStaticMarkup } from "react-dom/server";
import { expect, it } from "vitest";
import { PermissionFields } from "./permissions";
import { permissionPreview, RESOURCE_PERMISSIONS, resourceGrantPermissions } from "./logic";

it("limits resource grant edits while retaining identity management in principal creation", () => {
  const direct = ["produce", "manage_credentials", "manage_tenants"];
  const resource = renderToStaticMarkup(<PermissionFields resourceOnly value={direct} onChange={() => undefined} />);
  for (const permission of RESOURCE_PERMISSIONS) expect(resource).toContain(`value="${permission}"`);
  expect(resource).not.toContain('value="manage_credentials"');
  expect(resource).not.toContain('value="manage_tenants"');
  const principal = renderToStaticMarkup(<PermissionFields value={direct} onChange={() => undefined} />);
  expect(principal).toContain('value="manage_credentials"');
  expect(principal).toContain('value="manage_tenants"');
});

it("excludes identity authority from grant submissions and removal previews", () => {
  const direct = ["produce", "manage_credentials", "manage_tenants"];
  const editable = resourceGrantPermissions(direct);
  expect(editable).toEqual(["produce"]);
  const preview = permissionPreview(editable, ["consume"], []);
  expect(preview.removed).toEqual(["produce"]);
  expect(preview.effective).toEqual(["consume"]);
});

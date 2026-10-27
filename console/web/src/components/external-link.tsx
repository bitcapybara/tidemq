import type { ReactNode } from "react";
import { ExternalLink as Icon } from "lucide-react";

export function ExternalLink({
  href,
  children,
}: {
  href: string;
  children: ReactNode;
}) {
  let url: URL;
  try {
    url = new URL(href, window.location.origin);
  } catch {
    return null;
  }
  if (url.protocol !== "https:" && url.protocol !== "http:") return null;
  return (
    <a
      className="resource-link"
      href={url.href}
      target="_blank"
      rel="noopener noreferrer"
    >
      {children}
      <Icon size={13} />
    </a>
  );
}

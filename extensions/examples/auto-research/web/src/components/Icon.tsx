import type { ReactNode } from "react";

const paths = {
  plus: <path d="M12 5v14M5 12h14" />,
  arrow: <path d="M5 12h14m-5-5 5 5-5 5" />,
  back: <path d="m10 6-6 6 6 6M4 12h16" />,
  folder: <path d="M3 7a2 2 0 0 1 2-2h5l2 3h7a2 2 0 0 1 2 2v9H3Z" />,
  grid: (
    <>
      <rect x="3" y="3" width="7" height="7" rx="1.5" />
      <rect x="14" y="3" width="7" height="7" rx="1.5" />
      <rect x="3" y="14" width="7" height="7" rx="1.5" />
      <rect x="14" y="14" width="7" height="7" rx="1.5" />
    </>
  ),
  chat: (
    <path d="M5 4h14a2 2 0 0 1 2 2v10a2 2 0 0 1-2 2H9l-6 4V6a2 2 0 0 1 2-2Z" />
  ),
  paper: (
    <>
      <path d="M6 3h9l4 4v14H6Z" />
      <path d="M14 3v5h5M9 12h7M9 16h5" />
    </>
  ),
  branch: (
    <>
      <path d="M6 6v12m0-7c0-4 12-1 12-5M6 12c0 4 12 1 12 6" />
      <circle cx="6" cy="4" r="2" />
      <circle cx="6" cy="20" r="2" />
      <circle cx="18" cy="4" r="2" />
      <circle cx="18" cy="20" r="2" />
    </>
  ),
  settings: (
    <>
      <path d="M4 7h16M4 17h16" />
      <circle cx="8" cy="7" r="3" fill="currentColor" stroke="none" />
      <circle cx="16" cy="17" r="3" fill="currentColor" stroke="none" />
    </>
  ),
  archive: (
    <>
      <path d="M4 8v13h16V8M9 12h6" />
      <rect x="3" y="3" width="18" height="5" rx="1" />
    </>
  ),
  close: <path d="m6 6 12 12M6 18 18 6" />,
  trash: (
    <>
      <path d="M4 7h16M10 11v6M14 11v6M6 7l1 14h10l1-14M9 7V4h6v3" />
    </>
  ),
  check: <path d="m5 12 4 4L19 6" />,
  refresh: (
    <>
      <path d="M20 11a8 8 0 1 0-2 6M20 4v7h-7" />
    </>
  ),
  copy: (
    <>
      <rect x="8" y="8" width="12" height="12" rx="2" />
      <path d="M16 8V6a2 2 0 0 0-2-2H6a2 2 0 0 0-2 2v8a2 2 0 0 0 2 2h2" />
    </>
  ),
  search: (
    <>
      <circle cx="10" cy="10" r="6" />
      <path d="m15 15 5 5" />
    </>
  ),
  spark: (
    <>
      <path d="m12 3 2.8 6.2L21 12l-6.2 2.8L12 21l-2.8-6.2L3 12l6.2-2.8Z" />
    </>
  ),
  menu: <path d="M4 6h16M4 12h16M4 18h16" />,
  edit: (
    <>
      <path d="m14 5 5 5M4 20l4-1L21 6l-5-4L3 15Z" />
    </>
  ),
  send: (
    <>
      <path d="m3 3 18 9-18 9 4-9Zm4 9h14" />
    </>
  ),
  globe: (
    <>
      <circle cx="12" cy="12" r="9" />
      <ellipse cx="12" cy="12" rx="4" ry="9" />
      <path d="M3 12h18" />
    </>
  ),
  target: (
    <>
      <circle cx="12" cy="12" r="9" />
      <circle cx="12" cy="12" r="5" />
      <circle cx="12" cy="12" r="1" />
    </>
  ),
  laptop: (
    <>
      <rect x="5" y="3" width="14" height="13" rx="1.5" />
      <path d="m5 16-3 5h20l-3-5" />
    </>
  ),
} satisfies Record<string, ReactNode>;

export type IconName = keyof typeof paths;
export function Icon({ name, size = 20 }: { name: IconName; size?: number }) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.65"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      {paths[name]}
    </svg>
  );
}

import { useEffect, useId, useRef } from "react";
import { createPortal } from "react-dom";
import type { ReactNode } from "react";
import { Icon } from "./Icon";

export function Modal({
  title,
  description,
  children,
  onClose,
  busy = false,
  wide = false,
}: {
  title: string;
  description?: string;
  children: ReactNode;
  onClose: () => void;
  busy?: boolean;
  wide?: boolean;
}) {
  const ref = useRef<HTMLDialogElement>(null);
  const titleID = useId();
  useEffect(() => {
    const dialog = ref.current!;
    if (!dialog.open) dialog.showModal();
    return () => dialog.close();
  }, []);
  return createPortal(
    <dialog
      ref={ref}
      className={`modal ${wide ? "modal-wide" : ""}`}
      aria-labelledby={titleID}
      onCancel={(event) => {
        event.preventDefault();
        if (!busy) onClose();
      }}
    >
      <header className="modal-header">
        <div>
          <h2 id={titleID}>{title}</h2>
          {description && <p>{description}</p>}
        </div>
        <button
          type="button"
          className="icon-button"
          aria-label="关闭窗口"
          disabled={busy}
          onClick={onClose}
        >
          <Icon name="close" />
        </button>
      </header>
      {children}
    </dialog>,
    document.body,
  );
}

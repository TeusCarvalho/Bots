# -*- coding: utf-8 -*-
"""
Qwen2.5-7B-Instruct — Dev Helper GUI 🇧🇷
---------------------------------------------------
💬 Assistente de Programação Local com Interface:
    - Responde sempre em Português do Brasil
    - Especialista em Python, automações, Streamlit e planilhas Excel
    - Interface com botão "Copiar Código"
    - Modelo público e compatível com 8 GB
Autor: bb 😎
"""

import torch
import pyperclip
import tkinter as tk
from tkinter import scrolledtext, messagebox
from transformers import AutoTokenizer, AutoModelForCausalLM
from huggingface_hub import login

# =====================================================
# 🔐 Login no Hugging Face (opcional, mas seguro)
# =====================================================
login("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")

# =====================================================
# ⚙️ Configuração do modelo (Qwen2.5 em vez do GLM)
# =====================================================
MODEL_NAME = "Qwen/Qwen2.5-7B-Instruct"

print(f"🔍 Carregando modelo {MODEL_NAME} (pode levar 1-2 min)...")
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME, trust_remote_code=True)
model = AutoModelForCausalLM.from_pretrained(
    MODEL_NAME,
    device_map="auto",
    torch_dtype=torch.float16,
    trust_remote_code=True
)

# =====================================================
# 🧠 Contexto inicial (modo desenvolvedor)
# =====================================================
history = [
    {
        "role": "system",
        "content": (
            "Você é um assistente de programação brasileiro 🇧🇷. "
            "Responda sempre em Português do Brasil, de forma clara e didática. "
            "Você é especialista em Python, automações com planilhas Excel, KNIME e dashboards Streamlit. "
            "Quando o usuário pedir código, formate com ```python ... ``` e explique brevemente o que ele faz."
        ),
    }
]

# =====================================================
# 🎨 Interface Gráfica (Tkinter)
# =====================================================
root = tk.Tk()
root.title("🤖 Qwen Dev Helper - Assistente de Programação 🇧🇷")
root.geometry("850x600")
root.configure(bg="#1e1e1e")

chat_box = scrolledtext.ScrolledText(
    root, wrap=tk.WORD, bg="#252526", fg="#ffffff", font=("Consolas", 11)
)
chat_box.pack(padx=10, pady=10, fill=tk.BOTH, expand=True)
chat_box.insert(
    tk.END,
    "🤖 Qwen pronto! Digite sua dúvida abaixo e pressione Enviar.\n\n"
)
chat_box.config(state=tk.DISABLED)

entry = tk.Entry(root, bg="#2d2d30", fg="white", font=("Consolas", 11))
entry.pack(fill=tk.X, padx=10, pady=5)

frame_buttons = tk.Frame(root, bg="#1e1e1e")
frame_buttons.pack(pady=5)


def send_message():
    user_text = entry.get().strip()
    if not user_text:
        return

    chat_box.config(state=tk.NORMAL)
    chat_box.insert(tk.END, f"🧑 Você: {user_text}\n")
    chat_box.config(state=tk.DISABLED)
    entry.delete(0, tk.END)
    root.update()

    history.append({"role": "user", "content": user_text})

    # Geração da resposta
    inputs = tokenizer.apply_chat_template(
        history, add_generation_prompt=True, return_tensors="pt"
    ).to("cuda")
    outputs = model.generate(
        **inputs,
        max_new_tokens=400,
        temperature=0.7,
        top_p=0.9,
        do_sample=True,
    )
    response = tokenizer.decode(
        outputs[0][inputs["input_ids"].shape[-1]:],
        skip_special_tokens=True
    )

    chat_box.config(state=tk.NORMAL)
    chat_box.insert(tk.END, f"🤖 Qwen:\n{response}\n\n")
    chat_box.config(state=tk.DISABLED)
    chat_box.yview(tk.END)

    history.append({"role": "assistant", "content": response})


def copy_last_code():
    chat_text = chat_box.get("1.0", tk.END)
    if "```" not in chat_text:
        messagebox.showinfo("Copiar Código", "Nenhum bloco de código encontrado ainda.")
        return

    start = chat_text.rfind("```python")
    if start == -1:
        start = chat_text.rfind("```")
    end = chat_text.find("```", start + 3)

    if start != -1 and end != -1:
        code = chat_text[start + 9:end].strip()
        pyperclip.copy(code)
        messagebox.showinfo("Copiar Código", "Código copiado com sucesso! ✅")
    else:
        messagebox.showinfo("Copiar Código", "Nenhum código detectado.")


btn_send = tk.Button(
    frame_buttons,
    text="Enviar 🚀",
    command=send_message,
    bg="#0e639c",
    fg="white",
    font=("Segoe UI", 10, "bold"),
)
btn_send.pack(side=tk.LEFT, padx=5)

btn_copy = tk.Button(
    frame_buttons,
    text="Copiar Código 📋",
    command=copy_last_code,
    bg="#3a3d41",
    fg="white",
    font=("Segoe UI", 10),
)
btn_copy.pack(side=tk.LEFT, padx=5)

root.bind("<Return>", lambda event: send_message())

root.mainloop()

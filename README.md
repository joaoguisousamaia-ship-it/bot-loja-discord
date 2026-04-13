# Bot Loja Discord

Bot de loja para Discord com:
- Mensagem de produto com imagem e botao **Comprar**
- Criacao de canal privado de checkout ao clicar no botao
- Resumo da compra com botoes de pagamento, cupom e cancelamento
- Implementacao em Python usando discord.py

## 1. Instalar

```bash
pip install -r requirements.txt
```

## 2. Configurar

1. Copie `.env.example` para `.env`
2. Preencha os IDs e token

## 3. Rodar

```bash
python bot.py
```

## 4. Uso

1. No Discord, use o comando `/postar_produto`
2. O bot publica o card no canal configurado
3. Ao clicar em **Comprar**, o bot cria um canal privado com o resumo

## Permissoes necessarias do bot

- View Channels
- Send Messages
- Read Message History
- Manage Channels (para apagar checkout cancelado)
- Use Application Commands

## Arquivos principais

- `bot.py` (bot Python)
- `requirements.txt` (dependencias Python)

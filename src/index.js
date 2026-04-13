require("dotenv").config();
const {
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle,
  ChannelType,
  Client,
  EmbedBuilder,
  Events,
  GatewayIntentBits,
  PermissionFlagsBits,
  REST,
  Routes,
  SlashCommandBuilder,
} = require("discord.js");

const {
  BOT_TOKEN,
  CLIENT_ID,
  GUILD_ID,
  LOJA_CHANNEL_ID,
  TICKETS_CATEGORY_ID,
} = process.env;

if (!BOT_TOKEN || !CLIENT_ID || !GUILD_ID || !LOJA_CHANNEL_ID) {
  console.error("Variaveis obrigatorias ausentes no .env");
  process.exit(1);
}

const client = new Client({ intents: [GatewayIntentBits.Guilds] });

const product = {
  id: "discord_wl",
  nome: "DISCORD WL",
  preco: 2.5,
  estoque: 18,
  descricao: [
    "Contas Discord prontas para uso.",
    "Ja configuradas para sua diversao ou trabalho.",
    "Receba login completo com e-mail e senha.",
    "Entrega instantanea apos a compra.",
  ],
  imagem:
    "https://images.unsplash.com/photo-1614680376573-df3480f0c6ff?auto=format&fit=crop&w=1200&q=80",
};

function formatBRL(valor) {
  return `R$${valor.toFixed(2)}`;
}

function buildProductEmbed() {
  return new EmbedBuilder()
    .setColor(0x1f2328)
    .setTitle("Eclypse Vendas | Produto")
    .setDescription(
      [
        "**Produto premium para Discord**",
        ...product.descricao.map((linha) => `- ${linha}`),
        "",
        `**Nome:** ${product.nome}`,
        `**Preco:** ${formatBRL(product.preco)}`,
        `**Estoque:** ${product.estoque}`,
      ].join("\n")
    )
    .setImage(product.imagem);
}

function buildProductButtons() {
  return new ActionRowBuilder().addComponents(
    new ButtonBuilder()
      .setCustomId(`comprar_${product.id}`)
      .setLabel("Comprar")
      .setStyle(ButtonStyle.Success)
  );
}

function buildResumoEmbed(userId) {
  return new EmbedBuilder()
    .setColor(0x2f3136)
    .setTitle("Eclypse Vendas | Resumo da Compra")
    .setDescription(
      [
        `**Cliente:** <@${userId}>`,
        `**Produto:** ${product.nome}`,
        `**Valor unitario:** ${formatBRL(product.preco)}`,
        "**Quantidade:** 1",
        `**Total:** ${formatBRL(product.preco)}`,
        "",
        "**Produtos no carrinho:** 1",
        `**Valor a pagar:** ${formatBRL(product.preco)}`,
        "**Cupom adicionado:** Sem cupom",
      ].join("\n")
    );
}

function buildResumoButtons() {
  return new ActionRowBuilder().addComponents(
    new ButtonBuilder()
      .setCustomId("checkout_pagamento")
      .setLabel("Ir para o Pagamento")
      .setStyle(ButtonStyle.Success),
    new ButtonBuilder()
      .setCustomId("checkout_cupom")
      .setLabel("Adicionar Cupom de Desconto")
      .setStyle(ButtonStyle.Primary),
    new ButtonBuilder()
      .setCustomId("checkout_cancelar")
      .setLabel("Cancelar Compra")
      .setStyle(ButtonStyle.Danger)
  );
}

async function registerCommands() {
  const commands = [
    new SlashCommandBuilder()
      .setName("postar_produto")
      .setDescription("Posta o card do produto com botao de compra")
      .toJSON(),
  ];

  const rest = new REST({ version: "10" }).setToken(BOT_TOKEN);
  await rest.put(Routes.applicationGuildCommands(CLIENT_ID, GUILD_ID), {
    body: commands,
  });
}

client.once(Events.ClientReady, async () => {
  try {
    await registerCommands();
    console.log(`Bot online como ${client.user.tag}`);
  } catch (error) {
    console.error("Falha ao registrar comandos:", error);
  }
});

client.on(Events.InteractionCreate, async (interaction) => {
  if (interaction.isChatInputCommand()) {
    if (interaction.commandName === "postar_produto") {
      if (!interaction.memberPermissions?.has(PermissionFlagsBits.Administrator)) {
        await interaction.reply({
          content: "Apenas administradores podem usar este comando.",
          ephemeral: true,
        });
        return;
      }

      const lojaChannel = await interaction.guild.channels.fetch(LOJA_CHANNEL_ID);
      if (!lojaChannel || lojaChannel.type !== ChannelType.GuildText) {
        await interaction.reply({
          content: "Canal da loja invalido no .env.",
          ephemeral: true,
        });
        return;
      }

      await lojaChannel.send({
        embeds: [buildProductEmbed()],
        components: [buildProductButtons()],
      });

      await interaction.reply({
        content: "Produto postado com sucesso.",
        ephemeral: true,
      });
    }
    return;
  }

  if (!interaction.isButton()) {
    return;
  }

  if (interaction.customId.startsWith("comprar_")) {
    const existingCheckout = interaction.guild.channels.cache.find(
      (channel) =>
        channel.type === ChannelType.GuildText &&
        channel.topic === `checkout:${interaction.user.id}`
    );

    if (existingCheckout) {
      await interaction.reply({
        content: `Voce ja possui checkout aberto em ${existingCheckout}.`,
        ephemeral: true,
      });
      return;
    }

    const channelName = `compra-${interaction.user.username}`
      .toLowerCase()
      .replace(/[^a-z0-9-]/g, "")
      .slice(0, 20);

    const compraChannel = await interaction.guild.channels.create({
      name: channelName || `compra-${interaction.user.id.slice(0, 5)}`,
      type: ChannelType.GuildText,
      parent: TICKETS_CATEGORY_ID || null,
      topic: `checkout:${interaction.user.id}`,
      permissionOverwrites: [
        {
          id: interaction.guild.roles.everyone,
          deny: [PermissionFlagsBits.ViewChannel],
        },
        {
          id: interaction.user.id,
          allow: [
            PermissionFlagsBits.ViewChannel,
            PermissionFlagsBits.SendMessages,
            PermissionFlagsBits.ReadMessageHistory,
          ],
        },
        {
          id: interaction.client.user.id,
          allow: [
            PermissionFlagsBits.ViewChannel,
            PermissionFlagsBits.SendMessages,
            PermissionFlagsBits.ManageChannels,
            PermissionFlagsBits.ReadMessageHistory,
          ],
        },
      ],
      reason: `Checkout para ${interaction.user.tag}`,
    });

    await compraChannel.send({
      embeds: [buildResumoEmbed(interaction.user.id)],
      components: [buildResumoButtons()],
    });

    await interaction.reply({
      content: `Seu checkout foi criado em ${compraChannel}.`,
      ephemeral: true,
    });

    return;
  }

  if (interaction.customId === "checkout_pagamento") {
    await interaction.reply({
      content: "Pagamento: envie aqui o comprovante ou integre com seu gateway.",
      ephemeral: true,
    });
    return;
  }

  if (interaction.customId === "checkout_cupom") {
    await interaction.reply({
      content: "Cupom: neste exemplo, adicione sua logica de validacao de cupom.",
      ephemeral: true,
    });
    return;
  }

  if (interaction.customId === "checkout_cancelar") {
    await interaction.reply({
      content: "Compra cancelada. Este canal sera apagado em 5 segundos.",
      ephemeral: true,
    });

    setTimeout(async () => {
      if (interaction.channel && interaction.channel.deletable) {
        await interaction.channel.delete("Checkout cancelado pelo usuario");
      }
    }, 5000);
  }
});

client.login(BOT_TOKEN);

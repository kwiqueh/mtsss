const sheet = document.querySelector("#sheet");
const content = document.querySelector("#sheet-content");
const template = document.querySelector("#tariff-template");
let catalog;

const clean = (value = "") => value.replace(/<[^>]*>/g, "").replace(/^[^\p{L}\p{N}]+/u, "").trim();
const summary = (value = "") => clean(value).split("\n").filter(Boolean).slice(0, 4).join("\n");
const escapeHtml = (value = "") => value.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll('"', "&quot;").replaceAll("'", "&#039;");

async function loadCatalog() {
  if (!catalog) {
    const response = await fetch("../tariff_data.json");
    if (!response.ok) throw new Error("catalog unavailable");
    catalog = await response.json();
  }
  return catalog;
}

async function openCategory(category) {
  content.replaceChildren();
  document.querySelector("#sheet-kicker").textContent = "01 / ПРЕДЛОЖЕНИЯ";
  sheet.showModal();
  try {
    const data = await loadCatalog();
    const offers = Object.entries(data.TARIFFS).filter(([, item]) => item.category === category && item.active !== false);
    offers.forEach(([key, item]) => {
      const card = template.content.cloneNode(true);
      card.querySelector(".tariff-meta").textContent = item.region ? item.region.toUpperCase() : "РЕГИОН УТОЧНЯЕТСЯ";
      card.querySelector("h3").textContent = clean(item.title);
      card.querySelector(".tariff-copy").textContent = summary(item.description);
      card.querySelector("button").addEventListener("click", () => openFlow(key, item));
      content.append(card);
    });
  } catch {
    content.innerHTML = '<div class="flow"><h2>Каталог не загружен</h2><p>Запустите прототип через локальный веб-сервер из корня проекта.</p></div>';
  }
}

function openFlow(key, item) {
  document.querySelector("#sheet-kicker").textContent = "02 / ДАННЫЕ";
  content.innerHTML = `<section class="flow"><div class="progress"><i class="on"></i><i class="on"></i><i></i><i></i></div><h2>${escapeHtml(clean(item.title))}</h2><p>Чистый flow: услуга → минимальные данные → проверка → подтверждение.</p><p class="notice">Это визуальный прототип. Он не отправляет данные и не подключён к клиентской базе.</p><button class="primary" type="button" id="prototype-next">Посмотреть проверку <span>→</span></button></section>`;
  document.querySelector("#prototype-next").addEventListener("click", () => openReview(key, item));
}

function openReview(key, item) {
  document.querySelector("#sheet-kicker").textContent = "03 / ПРОВЕРКА";
  content.innerHTML = `<section class="flow"><div class="progress"><i class="on"></i><i class="on"></i><i class="on"></i><i></i></div><h2>Проверьте выбор</h2><p><strong>${escapeHtml(clean(item.title))}</strong></p><p>Регион и доступность будут подтверждены специалистом. Важные условия показываются до согласия.</p><p class="notice">Для запуска потребуется backend с Telegram initData validation, HTTPS URL и явное согласие на обработку данных.</p><button class="secondary" type="button" id="prototype-done">Готово</button></section>`;
  document.querySelector("#prototype-done").addEventListener("click", () => sheet.close());
}

document.querySelectorAll("[data-category]").forEach((button) => button.addEventListener("click", () => openCategory(button.dataset.category)));
document.querySelector("[data-scroll-catalog]").addEventListener("click", () => document.querySelector("#catalog").scrollIntoView());
document.querySelector("[data-open-consultation]").addEventListener("click", () => {
  sheet.showModal(); document.querySelector("#sheet-kicker").textContent = "КОНСУЛЬТАЦИЯ";
  content.innerHTML = '<section class="flow"><h2>Поговорим по делу.</h2><p>В рабочем Mini App здесь будет короткая форма: имя, телефон, удобное время и согласие.</p><p class="notice">Прототип не собирает и не отправляет персональные данные.</p></section>';
});

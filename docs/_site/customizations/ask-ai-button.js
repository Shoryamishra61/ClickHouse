(function () {
  'use strict';

  var BTN_ID = 'ch-ask-ai-btn';
  var ROW_ID = 'ch-ask-ai-row';
  var PAGE_ROW_ID = 'ch-page-ask-row';
  var PAGE_BTN_ID = 'ch-page-ask-btn';

  var sparkleSvg = '<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 18 18"'
    + ' class="ch-ai-icon size-4 shrink-0 text-gray-700">'
    + '<g fill="currentColor">'
    + '<path d="M5.658,2.99l-1.263-.421-.421-1.263c-.137-.408-.812-.408-.949,0l-.421,1.263-1.263,.421c-.204,.068-.342,.259-.342,.474s.138,.406,.342,.474l1.263,.421,.421,1.263c.068,.204,.26,.342,.475,.342s.406-.138,.475-.342l.421-1.263,1.263-.421c.204-.068,.342-.259,.342-.474s-.138-.406-.342-.474Z" fill="currentColor" data-stroke="none" stroke="none"></path>'
    + '<polygon points="9.5 2.75 11.412 7.587 16.25 9.5 11.412 11.413 9.5 16.25 7.587 11.413 2.75 9.5 7.587 7.587 9.5 2.75" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5"></polygon>'
    + '</g></svg>';

  function trackDocsAi(entryPoint, pageContext) {
    var properties = {
      entry_point: entryPoint,
      page_context: String(Boolean(pageContext)),
      interaction: 'click'
    };
    var track = function () {
      if (window.galaxy && typeof window.galaxy.track === 'function') {
        window.galaxy.track('docs.docsai.open', properties);
      }
    };

    if (window.galaxy && typeof window.galaxy.track === 'function') track();
    else window.addEventListener('galaxy:ready', track, { once: true });
  }

  // Wait briefly for Kapa to mount (it's loaded async), then open. Passing a
  // page query prefills context without submitting a synthetic question.
  function openKapa(query, submit) {
    var opts = { mode: 'ai' };
    if (query) {
      opts.query = query;
      opts.submit = Boolean(submit);
    }
    var attempts = 0;
    var iv = setInterval(function () {
      attempts++;
      if (window.Kapa && typeof window.Kapa.open === 'function') {
        clearInterval(iv);
        window.Kapa.open(opts);
      } else if (attempts > 60) {
        clearInterval(iv);
      }
    }, 50);
  }

  function pageContextQuery(title) {
    return 'I am reading “' + title + '” (' + window.location.href
      + '). Use this page as context for my question:\n\n';
  }

  function makeButton(id, label, entryPoint, pageTitle) {
    var btn = document.createElement('button');
    btn.id = id;
    btn.type = 'button';
    btn.className = 'ch-ask-ai-button';
    btn.setAttribute('aria-label', pageTitle ? 'Ask AI about ' + pageTitle : 'Ask AI');
    btn.setAttribute('aria-keyshortcuts', 'Meta+I');
    btn.innerHTML = sparkleSvg + '<span class="ch-ask-ai-label">' + label + '</span>';
    btn.addEventListener('click', function (e) {
      e.stopPropagation();
      trackDocsAi(entryPoint, Boolean(pageTitle));
      openKapa(pageTitle ? pageContextQuery(pageTitle) : undefined, false);
    });
    return btn;
  }

  function injectSidebarButton() {
    if (document.getElementById(BTN_ID)) return true;

    var searchBar = document.getElementById('search-bar-entry');
    if (!searchBar) return false;

    var host = searchBar.closest('div.flex.flex-col.gap-4.mt-6') || searchBar.parentNode;
    if (!host) return false;

    var row = document.createElement('div');
    row.id = ROW_ID;
    row.appendChild(makeButton(BTN_ID, 'Ask AI <small>· docs assistant</small><kbd>⌘I</kbd>', 'sidebar', ''));
    host.appendChild(row);
    return true;
  }

  function injectPageButton() {
    var main = document.querySelector('main');
    var title = main && main.querySelector('h1');
    if (!title) return false;
    if (title.parentNode && title.parentNode.id === PAGE_ROW_ID) return true;

    var row = document.createElement('div');
    row.id = PAGE_ROW_ID;
    title.parentNode.insertBefore(row, title);
    row.appendChild(title);
    row.appendChild(makeButton(PAGE_BTN_ID, 'Ask about this page', 'page-title', title.textContent.trim()));
    return true;
  }

  function bindShortcut() {
    if (document.documentElement.dataset.chAskAiShortcutBound) return;
    document.documentElement.dataset.chAskAiShortcutBound = 'true';
    document.addEventListener('keydown', function (event) {
      if (event.defaultPrevented || !(event.metaKey || event.ctrlKey) || event.key.toLowerCase() !== 'i') return;
      event.preventDefault();
      trackDocsAi('keyboard-shortcut', false);
      openKapa();
    });
  }

  function init() {
    injectSidebarButton();
    injectPageButton();
    bindShortcut();

    var observer = new MutationObserver(function () {
      injectSidebarButton();
      injectPageButton();
    });
    observer.observe(document.documentElement, { childList: true, subtree: true });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();

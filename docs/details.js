function openFragment() {
  if (!location.hash) return;

  const target = document.getElementById(location.hash.slice(1));
  if (!target) return;

  for (let element = target; element; element = element.parentElement) {
    if (element instanceof HTMLDetailsElement) element.open = true;
  }
  target.scrollIntoView();
}

window.addEventListener("hashchange", openFragment);
document.addEventListener("click", (event) => {
  const link = event.target instanceof Element && event.target.closest('a[href^="#"]');
  if (link && link.hash === location.hash) openFragment();
});
openFragment();

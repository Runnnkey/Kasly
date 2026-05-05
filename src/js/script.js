const menuBtn = document.getElementById("menuBtn");
const menuBtn2 = document.getElementById("menuBtn2");
const sidebar = document.getElementById("sidebar");

menuBtn.addEventListener("click", () => {
    sidebar.classList.toggle("-translate-x-full");
});

menuBtn2.addEventListener("click", () => {
    sidebar.classList.toggle("-translate-x-full");
});
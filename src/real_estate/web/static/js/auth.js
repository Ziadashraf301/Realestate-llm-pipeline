/**
 * Enterprise Authentication Client Module.
 * Connects with /api/v1/auth/login & /api/v1/auth/register
 * Manages JWT tokens, user profiles, and session security.
 */

document.addEventListener('DOMContentLoaded', () => {
    // 1. If already authenticated, redirect to discovery dashboard
    const existingToken = localStorage.getItem('re_token');
    if (existingToken) {
        window.location.href = '/';
        return;
    }

    const tabLogin = document.getElementById('tab-login');
    const tabRegister = document.getElementById('tab-register');
    const formLogin = document.getElementById('form-login');
    const formRegister = document.getElementById('form-register');
    const alertBox = document.getElementById('auth-alert');

    // Tab switcher
    tabLogin.addEventListener('click', () => {
        tabLogin.classList.add('active');
        tabRegister.classList.remove('active');
        formLogin.classList.remove('hidden');
        formRegister.classList.add('hidden');
        clearAlert();
    });

    tabRegister.addEventListener('click', () => {
        tabRegister.classList.add('active');
        tabLogin.classList.remove('active');
        formRegister.classList.remove('hidden');
        formLogin.classList.add('hidden');
        clearAlert();
    });

    function showAlert(msg, isError = true) {
        alertBox.textContent = msg;
        alertBox.className = `alert ${isError ? 'alert-error' : 'alert-success'}`;
        alertBox.classList.remove('hidden');
    }

    function clearAlert() {
        alertBox.textContent = '';
        alertBox.classList.add('hidden');
    }

    // Handle Login Submit
    formLogin.addEventListener('submit', async (e) => {
        e.preventDefault();
        clearAlert();

        const username = document.getElementById('login-username').value.trim();
        const password = document.getElementById('login-password').value;
        const btn = formLogin.querySelector('button[type="submit"]');

        if (!username || !password) {
            showAlert('يرجى إدخال اسم المستخدم وكلمة المرور');
            return;
        }

        btn.disabled = true;
        btn.textContent = 'جاري التحقق...';

        try {
            const resp = await fetch('/api/v1/auth/login', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ username, password })
            });

            const data = await resp.json();

            if (resp.ok && data.access_token) {
                localStorage.setItem('re_token', data.access_token);
                localStorage.setItem('re_user', JSON.stringify(data.user));
                showAlert('تم تسجيل الدخول بنجاح! جاري تحويلك...', false);
                setTimeout(() => { window.location.href = '/'; }, 600);
            } else {
                showAlert(data.detail || 'بيانات الدخول غير صحيحة');
                btn.disabled = false;
                btn.textContent = 'تسجيل الدخول';
            }
        } catch (err) {
            showAlert('حدث خطأ أثناء الاتصال بالخادم. يرجى المحاولة لاحقاً.');
            btn.disabled = false;
            btn.textContent = 'تسجيل الدخول';
        }
    });

    // Handle Register Submit
    formRegister.addEventListener('submit', async (e) => {
        e.preventDefault();
        clearAlert();

        const username = document.getElementById('reg-username').value.trim();
        const email = document.getElementById('reg-email').value.trim();
        const password = document.getElementById('reg-password').value;
        const role = document.getElementById('reg-role').value;
        const btn = formRegister.querySelector('button[type="submit"]');

        if (!username || !email || !password) {
            showAlert('يرجى ملء جميع الحقول المطلوبة');
            return;
        }

        if (password.length < 6) {
            showAlert('كلمة المرور يجب أن لا تقل عن 6 أحرف');
            return;
        }

        btn.disabled = true;
        btn.textContent = 'جاري إنشاء الحساب...';

        try {
            const resp = await fetch('/api/v1/auth/register', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ username, email, password, role })
            });

            const data = await resp.json();

            if (resp.ok && data.access_token) {
                localStorage.setItem('re_token', data.access_token);
                localStorage.setItem('re_user', JSON.stringify(data.user));
                showAlert('تم إنشاء الحساب بنجاح! جاري تحويلك...', false);
                setTimeout(() => { window.location.href = '/'; }, 600);
            } else {
                showAlert(data.detail || 'فشل إنشاء الحساب، قد يكون الاسم أو البريد مسجلاً مسبقاً');
                btn.disabled = false;
                btn.textContent = 'إنشاء حساب جديد';
            }
        } catch (err) {
            showAlert('حدث خطأ أثناء الاتصال بالخادم. يرجى المحاولة لاحقاً.');
            btn.disabled = false;
            btn.textContent = 'إنشاء حساب جديد';
        }
    });
});

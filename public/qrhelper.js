/**
 * 本地二维码渲染 —— 不依赖任何外部服务（离线/云端都能出图）
 * 用法：renderQRInto(容器元素, 文本, 像素尺寸)
 */
function renderQRInto(box, text, size) {
  size = size || 130;
  try {
    const qr = qrcode(0, 'M');          // 0 = 自动选版本，M = 中等纠错
    qr.addData(String(text));
    qr.make();
    box.innerHTML = qr.createSvgTag({ cellSize: 4, margin: 3, scalable: true });
    const svg = box.querySelector('svg');
    if (!svg) return false;
    svg.setAttribute('width', size);
    svg.setAttribute('height', size);
    svg.style.cssText = 'display:block;margin:0 auto;background:#fff;padding:6px;border-radius:8px;width:' + size + 'px;height:' + size + 'px';
    return true;
  } catch (e) {
    console.error('二维码生成失败', e);
    return false;
  }
}
